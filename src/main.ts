import { existsSync, readFileSync, statSync } from "fs";
import { mkdir, stat, writeFile } from "fs/promises";
import { dirname, join } from "path";

type PlexMeta = {
	name: string;
	collections: string[];
	indexes: Record<string, string>;
};

export default class PlexDB {
	/**
	 * Create new Plex database at `path`.
	 */
	public static async createNew (path: string) {
		if (typeof path !== "string") throw new Error("Supply a valid path string!");
		if (!(await stat(dirname(path))).isDirectory()) throw new Error("Cannot create DB here! " + path);

		await mkdir(path);
		await Promise.all([
			writeFile(join(path, ".plexdb"), JSON.stringify({
				collections: [],
				indexes: {},
				name: "db0",
			} as PlexMeta), "utf-8"),
			mkdir(join(path, "index")),
			mkdir(join(path, "data")),
		]);
	};

	private readonly key: [publickey: string, privatekey: string];
	private readonly root: string;

	private meta: PlexMeta;

	constructor (rootpath: string) {
		if (!statSync(rootpath).isDirectory()) throw new Error("Path is not a directory!");
		if (!existsSync(join(rootpath, ".plexdb"))) throw new Error("Path is not a Plex database!");
		this.root = rootpath;
		
		this.meta = JSON.parse(readFileSync(join(rootpath, ".plexdb"), "utf-8"));
	};
};
