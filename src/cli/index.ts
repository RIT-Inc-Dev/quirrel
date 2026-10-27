#!/usr/bin/env node

import { type Command, program } from "commander";
import pack from "../../package.json";
import registerCI from "./commands/ci";
import registerDetectCron from "./commands/detect-cron";
import registerRun from "./commands/index";
import registerUI from "./commands/ui";
import registerUpdateCron from "./commands/update-cron";

program.version(pack.version);

const command = program as Command;

registerRun(command);
registerUI(command);
registerCI(command);
registerDetectCron(command);
registerUpdateCron(command);

program.parse(process.argv);
