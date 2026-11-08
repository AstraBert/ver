import { defineCollection } from "astro:content";
// `z` re-exported from `astro:content` is deprecated; import it from
// `astro/zod` (the pattern nimbus-docs' own schema helpers document).
import { z } from "astro/zod";
import { docsCollection, partialsCollection } from "@cloudflare/nimbus-docs/content";

export const collections = {
  docs: defineCollection(
    docsCollection({
      schemaFields: {
        // Nimbus docs are agent-friendly by default. Set `audience: human`
        // to flag a page that's written primarily for human readers.
        audience: z.literal("human").optional(),
      },
    }),
  ),
  blog: defineCollection(
    docsCollection({
      base: "blog",
      schemaFields: {
        // Blog posts have an author and publication date (optional for index pages)
        author: z.string().optional(),
        pubDate: z.coerce.date().optional(),
        // Optional tags for categorization
        tags: z.array(z.string()).optional(),
      },
    }),
  ),
  partials: defineCollection(partialsCollection()),
};
