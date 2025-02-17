import { log } from "console";
import PlexServer from "./Server";
import { randomBytes, UUID } from "crypto";
import { resolve } from "path";

const password = "ZNlCfjv3whHeNiT0hC3CkrNe06T/CuyUEPsIcyXMio2ps/1AcHsDhmNcQhltNRNkGWHfFJ5Kj+5bs4kcpHuEB/uS/OJ9qrA8bfdUH74TbKfwWG218w7UpWEux/hqUsOQofvXey9JGIAVnF1GlVrWzIxNTDFD+rfrmCLsMnYaIQQ=";

const db = new PlexServer(resolve("db"));

class User {
	public username: string;
	public id: UUID;
}

(async () => {
	const userCollection = db.collection("user", new User(), {
		id: {},
		username: {
			required: true,
			default() {
				return randomBytes(32).toString("base64")
			},
			index: true,
			unique: true,
		}
	});

	const user = await userCollection.findOne({
		username: "UM2WLsIxHnCKnRkp5TRD3RXtyjjdGvSty5Zq4Frp4AU="
	});

	log(user);

})();