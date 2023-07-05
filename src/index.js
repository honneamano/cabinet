import * as fs from "fs/promises";
import _ from "lodash";
import toml from "toml";

import { BureauOfTransmission } from "common/BureauOfTransmission";


(async function(){

    const configFile = await fs.readFile(_.get(process.argv, 2));
    const configFileParsed = toml.parse(configFile);

    const zeromqConfig = _.get(configFileParsed, "bureau-of-transmission");

    const bureauOfTransmission = 
        new BureauOfTransmission({ config: zeromqConfig });

    await bureauOfTransmission.ready();
})();
