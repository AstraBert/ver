/**
 * MDX globals registry — components available inside MDX without `import`.
 * Wired via `<Content components={components} />` in `[...slug].astro`.
 * Add new components here as you build (or install) them.
 */

import { Aside } from "./components/ui/aside";
import CopyCode from "./components/CopyCode.astro";
import Logo from "./components/Logo.astro";
import Render from "./components/Render.astro";
import { Card } from "./components/ui/card";
import { CardGrid } from "./components/ui/card-grid";
import { PackageManagers } from "./components/ui/package-managers";
import { Step, Steps } from "./components/ui/steps";
import { Tabs, TabItem } from "./components/ui/tabs";
import { Frame } from "./components/ui/frame";
import { Accordion, AccordionContent, AccordionGroup, AccordionTrigger } from "./components/ui/accordion";

export const components = {
  Aside,
  Card,
  CardGrid,
  CopyCode,
  Logo,
  PackageManagers,
  Render,
  Step,
  Steps,
  TabItem,
  Tabs,
  Frame,
  Accordion,
  AccordionTrigger,
  AccordionContent,
  AccordionGroup,
};
