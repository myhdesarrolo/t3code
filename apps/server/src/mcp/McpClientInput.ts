import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import { McpSchema } from "effect/ai";

/**
 * Where one tool call leaves its request for the client's input. A sessionless client is asked
 * by answering the call with that request; it calls the tool again with the person's answer.
 * `McpHttpServer` provides this around every call and sends what the handler left here.
 */
export class McpClientInput extends Context.Service<
  McpClientInput,
  { readonly request: Ref.Ref<McpSchema.InputRequired | undefined> }
>()("t3/mcp/McpClientInput") {}

/**
 * "asked": the request went out with this call's answer, so the handler ends now and runs again
 * when the client calls back. "unavailable": this client cannot ask a person anything.
 */
export type Approval = "accepted" | "declined" | "asked" | "unavailable";

const consentForm = { type: "object", properties: {} } as const;
const decodeElicitRequest = Schema.decodeEffect(McpSchema.ElicitRequestFormParams);

/**
 * Asks the person behind the calling MCP client to approve `message`, by MCP elicitation.
 * `state` names what is being approved: an answer counts only for the state it was given for,
 * so asking again with another state asks the person again.
 */
export const askApproval = Effect.fn("McpClientInput.askApproval")(function* (input: {
  readonly key: string;
  readonly state: string;
  readonly message: string;
}): Effect.fn.Return<Approval> {
  const context = Option.getOrUndefined(yield* Effect.serviceOption(McpSchema.McpRequestContext));
  const elicitation = context?.clientCapabilities.elicitation;
  // The request is a form. A client that names no mode takes forms; one naming only URLs does not.
  if (
    context === undefined ||
    elicitation === undefined ||
    (elicitation.form === undefined && elicitation.url !== undefined)
  ) {
    return "unavailable";
  }
  const params = { mode: "form", message: input.message, requestedSchema: consentForm } as const;

  // A session: the server asks over it and waits here for the answer.
  const session = Option.getOrUndefined(yield* Effect.serviceOption(McpSchema.McpServerClient));
  if (session !== undefined) {
    const request = yield* decodeElicitRequest(params).pipe(Effect.orDie);
    return yield* session.getClient.pipe(
      Effect.flatMap((client) => client.elicit(request)),
      Effect.scoped,
      Effect.map(({ action }) => (action === "accept" ? "accepted" : "declined")),
      // A request that ends without an answer is not an approval.
      Effect.catchCause(() => Effect.succeed("declined" as const)),
    );
  }

  const pending = Option.getOrUndefined(yield* Effect.serviceOption(McpClientInput));
  if (pending === undefined) return "unavailable";
  const answer = context.inputResponses?.[input.key];
  if (answer !== undefined && context.requestState === input.state) {
    return answer.action === "accept" ? "accepted" : "declined";
  }
  yield* Ref.set(
    pending.request,
    new McpSchema.InputRequired({
      inputRequests: { [input.key]: { method: "elicitation/create", params } },
      requestState: input.state,
    }),
  );
  return "asked";
});
