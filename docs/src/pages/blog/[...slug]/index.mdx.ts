import { mdxRoute } from "@cloudflare/nimbus-docs/agent-endpoints";

export const { GET, getStaticPaths } = mdxRoute({ collection: "blog" });
