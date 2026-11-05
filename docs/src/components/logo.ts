/**
 * The site's logo, drawn once for every place it appears: the header and the
 * docs layout (Logo.astro) and the social cards (src/pages/og/_card.ts).
 * Replace the drawing here to rebrand them all. Keep it a single-colour
 * mark; each place fills it with its own text colour.
 *
 * The mark: a "V" drawn as a git graph — two branches curving down from
 * their tip nodes, merging into one trunk that ends at the V's point. The
 * rings are wound so the default nonzero fill-rule cuts their holes.
 */
export const logo = {
  /** The drawing's box; callers size it by height and keep the ratio. */
  width: 64,
  height: 40,
  paths: [
    // Left branch, tip node to the merge.
    "M 12.5 6 L 12.53 7.19 L 12.63 8.36 L 12.78 9.49 L 12.99 10.6 L 13.25 11.68 L 13.57 12.72 L 13.93 13.73 L 14.34 14.71 L 14.78 15.65 L 15.27 16.56 L 15.79 17.44 L 16.34 18.28 L 16.93 19.08 L 17.53 19.85 L 18.17 20.59 L 18.82 21.29 L 19.49 21.95 L 20.17 22.58 L 20.87 23.17 L 21.58 23.73 L 22.29 24.26 L 23.01 24.75 L 23.74 25.2 L 24.46 25.62 L 25.19 26 L 25.91 26.35 L 26.62 26.67 L 27.34 26.95 A 2.5 2.5 0 0 1 29.06 22.25 L 28.54 22.05 L 28 21.81 L 27.45 21.54 L 26.89 21.25 L 26.32 20.92 L 25.75 20.56 L 25.17 20.17 L 24.6 19.75 L 24.04 19.31 L 23.48 18.83 L 22.94 18.33 L 22.41 17.81 L 21.89 17.25 L 21.39 16.67 L 20.91 16.07 L 20.46 15.44 L 20.03 14.78 L 19.62 14.1 L 19.25 13.4 L 18.9 12.68 L 18.59 11.93 L 18.32 11.16 L 18.08 10.36 L 17.87 9.54 L 17.71 8.69 L 17.6 7.82 L 17.52 6.92 L 17.5 6 A 2.5 2.5 0 0 1 12.5 6 Z",
    // Right branch, tip node to the merge.
    "M 46.5 6 L 46.48 6.92 L 46.4 7.82 L 46.29 8.69 L 46.13 9.54 L 45.92 10.36 L 45.68 11.16 L 45.41 11.93 L 45.1 12.68 L 44.75 13.4 L 44.38 14.1 L 43.97 14.78 L 43.54 15.44 L 43.09 16.07 L 42.61 16.67 L 42.11 17.25 L 41.59 17.81 L 41.06 18.33 L 40.52 18.83 L 39.96 19.31 L 39.4 19.75 L 38.83 20.17 L 38.25 20.56 L 37.68 20.92 L 37.11 21.25 L 36.55 21.54 L 36 21.81 L 35.46 22.05 L 34.94 22.25 A 2.5 2.5 0 0 1 36.66 26.95 L 37.38 26.67 L 38.09 26.35 L 38.81 26 L 39.54 25.62 L 40.26 25.2 L 40.99 24.75 L 41.71 24.26 L 42.42 23.73 L 43.13 23.17 L 43.83 22.58 L 44.51 21.95 L 45.18 21.29 L 45.83 20.59 L 46.47 19.85 L 47.07 19.08 L 47.66 18.28 L 48.21 17.44 L 48.73 16.56 L 49.22 15.65 L 49.66 14.71 L 50.07 13.73 L 50.43 12.72 L 50.75 11.68 L 51.01 10.6 L 51.22 9.49 L 51.37 8.36 L 51.47 7.19 L 51.5 6 A 2.5 2.5 0 0 1 46.5 6 Z",
    // The merged trunk; its round bottom cap is the V's point.
    "M 29.5 26 L 29.5 36.5 A 2.5 2.5 0 0 1 34.5 36.5 L 34.5 26 A 2.5 2.5 0 0 1 29.5 26 Z",
    // Branch-tip commit nodes (rings).
    "M 10.6 6 A 4.4 4.4 0 1 1 19.4 6 A 4.4 4.4 0 1 1 10.6 6 Z M 12.8 6 A 2.2 2.2 0 1 0 17.2 6 A 2.2 2.2 0 1 0 12.8 6 Z",
    "M 44.6 6 A 4.4 4.4 0 1 1 53.4 6 A 4.4 4.4 0 1 1 44.6 6 Z M 46.8 6 A 2.2 2.2 0 1 0 51.2 6 A 2.2 2.2 0 1 0 46.8 6 Z",
    // The merge commit (ring).
    "M 27.2 27 A 4.8 4.8 0 1 1 36.8 27 A 4.8 4.8 0 1 1 27.2 27 Z M 29.6 27 A 2.4 2.4 0 1 0 34.4 27 A 2.4 2.4 0 1 0 29.6 27 Z",
  ],
};
