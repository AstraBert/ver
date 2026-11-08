import { markdownRoute } from "@cloudflare/nimbus-docs/agent-endpoints";

// @ts-expect-error ts(2554): the extra argument is fine
export const { GET, getStaticPaths } = markdownRoute({ collection: "blog" });
