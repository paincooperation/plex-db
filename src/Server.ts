import { log } from "console";
import { generateKeyPairSync, KeyObject, privateDecrypt, randomBytes, randomUUID, UUID } from "crypto";
import * as EventEmitter from "events";
import { existsSync, statSync, readFileSync, lutimesSync } from "fs";
import { stat, mkdir, writeFile, readFile, readdir, rm } from "fs/promises";
import { normalize, resolve } from "path";
import { dirname, join } from "path/posix";
import { Server, Socket } from "socket.io";

export type PlexMeta = {
	name: string;
	indexes: Record<string, string>;
};

export default class PlexServer {
	/**
	 * Create new Plex database at `path`.
	 */
	public static async createNew(path: string) {
		if (typeof path !== "string") throw new Error("Supply a valid path string!");
		if (!(await stat(dirname(path))).isDirectory()) throw new Error("Cannot create DB here! " + path);

		const {publicKey, privateKey} = generateKeyPairSync('rsa',{modulusLength: 2048});

		if (!existsSync(path))
			await mkdir(path);
		await Promise.all([
			writeFile(join(path, ".plexmeta"), JSON.stringify({
				collections: [],
				indexes: {},
				name: "db0",
			} as PlexMeta), "utf-8"),
			mkdir(join(path, "index")),
			mkdir(join(path, "data")),
		]);
	};

	public readonly root: string;
	private meta: PlexMeta;
	private closing: boolean = false;

	public storage = {
		cache: new Map <string, any> (),
		cacheClear: setInterval(() => {
			if (this.storage.cache.size < 10000) return;
			var keys = this.storage.cache.keys();
			while (this.storage.cache.size >= 10000)
				for (let i = 0; i < 200; i++)
					this.storage.cache.delete(keys.next().value);
		}, 50),

		/**
		 * Prefixes paths with the DB folder and throws if the requested item still ends up outside of DB
		 * @param path Any string path
		 * @returns Prefixed string path
		 */
		prefix: (path: string) => {
			var p = resolve(normalize(join(this.root, path)));
			if (!p.startsWith(this.root)) throw new Error("Requested path is outside of the DB");
			return p;
		},
		read: async (path : string): Promise<any> => {
			if (this.storage.cache.has(path)) return this.storage.cache[path];
			path = this.storage.prefix(path);

			if (!existsSync(path)) return void 0;
			return JSON.parse(
				await readFile(
					path,
					"utf-8"
				)
			);
		},
		write: async (path : string, data : any) => {
			this.storage.cache[path] = data;
			path = this.storage.prefix(path);
			if (!existsSync(dirname(path)))
				await mkdir(dirname(path), {recursive: true});
			return await writeFile(
				path,
				JSON.stringify(data),
				"utf-8"
			);
		},
		exists: (path : string): boolean => {
			return this.storage.cache.has(path) ? true : existsSync(this.storage.prefix(path));
		},
		list: async (path : string): Promise <string[]> => {
			if (!this.storage.exists(path)) return void 0;
			return await readdir(this.storage.prefix(path), "utf-8");
		},
		tree: async (path : string) => {
			if (!this.storage.exists(path)) return void 0;
			return await readdir(this.storage.prefix(path), {
				recursive: true,
				encoding: "utf-8",
			});
		},
		remove: async (path : string) => {
			this.storage.cache.delete(this.storage.prefix(path));
			if (this.storage.exists(path)) await rm(this.storage.prefix(path), {
				force: true,
				recursive: true,
			});
		},
		makedir: async (path : string) => {
			await mkdir(this.storage.prefix(path), {recursive: true});
		}
	}

	public events = new EventEmitter<{
		"close": [];
	}>();

	private readonly autosave = setInterval(() => {
		if (this.root && this.meta)
			this.storage.write(join(".plexmeta"), this.meta);
	}, 5000);

	constructor (db_folfer: string) {
		this.root = db_folfer;
		this.meta = JSON.parse(readFileSync(join(this.root, ".plexmeta"), "utf-8"));

		this.events.on("close", () => {});
	};

	public async close () {
		this.closing = true;
		this.events.emit("close");
		clearInterval(this.autosave);
		await this.storage.write(join(".plexmeta"), this.meta);
	};

	public collection <T extends Data, S extends Schema<T>, C> (name: string, dummy: T, schema: S) {
		return new Collection(name, dummy, schema, this);
	};
};

function pathsafe (data: any): string {
	return Buffer.from(JSON.stringify(data), "utf-8").toString("hex");
};

function wait (ms: number) {
	return new Promise<void>(res => {
		setTimeout(() => {
			res();
		}, ms);
	})
}

type Data = object & {id: UUID};
type Schema<T extends Data> = {
	[K in keyof T]: Partial<{
		index: boolean;
		unique: boolean;
		default?: T[K] | (() => (T[K] | Promise<T[K]>)),
		required: boolean;
	}>;
}
class Collection <T extends Data, S extends Schema<T>> {
	public schema: S;
	public name: string;
	public dummy: T;
	public readonly db: PlexServer;

	constructor (name: string, dummy: T, schema: S, db: PlexServer) {
		this.dummy = dummy;
		this.name = name;
		this.schema = schema;
		this.schema[("id" as any)] = {
			index: true,
			uniqure: true,
			default: randomUUID,
			required: true,
		};

		this.db = db;

		db.storage.makedir(join("data", name));
		db.storage.makedir(join("index", name));

		for (let key in schema) {
			if (key === "id") {
				continue
			} else {
				if (schema[key].index) {
					db.storage.makedir(join("index", name, key));
				};
			};
		};
	};

	public async findAll(data: Partial<T>): Promise<T[]> {
		const candidates = new Set<UUID>();
		const results: T[] = [];

		await Promise.all(Object.keys(data).map(async (key) => {
			if (key === "id") return;
			if (this.schema[key]?.index) {
				const indexPath = join("index", this.name, key, pathsafe(data[key]));
				if (!this.db.storage.exists(indexPath)) return;
				const fileContent = await this.db.storage.read(indexPath) as UUID[] | UUID;
	
				if (this.schema[key].unique) {
					candidates.add(fileContent as UUID);
				} else {
					(fileContent as UUID[]).forEach(uuid => candidates.add(uuid));
				};
			};
		}));
	
		// Check each candidate for full data match
		for (const candidate of candidates) {
			const obj = await this.db.storage.read(join("data", this.name, candidate)) as T;

			// Ensure all provided fields match
			if (Object.keys(data).every(key => data[key] === obj[key])) {
				results.push(obj);
			};
		};
	
		return results;
	};

	public async findOne(data: Partial<T>): Promise<T | null> {
		const candidates = new Set<UUID>();

		await Promise.all(Object.keys(data).map(async (key) => {
			if (key === "id") return;
			if (this.schema[key]?.index) {
				const indexPath = join("index", this.name, key, pathsafe(data[key]));
				if (!this.db.storage.exists(indexPath)) return;
				const fileContent = await this.db.storage.read(indexPath) as UUID | UUID[];

				if (this.schema[key].unique) {
					candidates.add(fileContent as UUID);
				} else {
					(fileContent as UUID[]).forEach(uuid => candidates.add(uuid));
				};
			};
		}));

		for (const candidate of candidates) {
			const obj = await this.db.storage.read(join("data", this.name, candidate)) as T;

			if (Object.keys(data).every(key => data[key] === obj[key])) {
				return obj;
			};
		};

		return void 0;
	};	

	public async delete (data: T) {
		await Promise.all([
			this.db.storage.remove(join("data", this.name, data.id)),
			...Object.keys(data).map(async key => {
				if (key === "id") return;
				if (this.schema[key].index) {
					await this.db.storage.remove(
						join("index", this.name, key, pathsafe(data[key]))
					);
				};
			})
		]);
	};

	/**
	 * 
	 * @param task 
	 * @param runtime predicted runtime in milisecconds
	 */
	public async queryAll (task: (data: T) => Promise<any>, runtime: number = 20) {
		let entries = Array.from((
			await this.db.storage.list(join("data", this.name))
		).entries());

		return await Promise.all(entries.map(async ([index, id]) => {
			await wait(runtime * index);
			return task((await this.db.storage.read(join("data", this.name, id))) as T);
		}));
	};

	public async get (id: UUID) {
		return await this.db.storage.read(join("data", this.name, id)) as T;
	};

	public async write(data: T) {
		return Promise.all([
			this.db.storage.write(join("data", this.name, data.id), data),
			...Object.keys(data).map(async key => {
				if (key === "id") return;
				if (this.schema[key].index) {
					const indexPath = join("index", this.name, key, pathsafe(data[key]));

					if (this.schema[key].unique) {
						await this.db.storage.write(indexPath, data.id);
					} else {
						let ids = (await this.db.storage.read(indexPath)) as UUID[] || [];
						ids.push(data.id);
						await this.db.storage.write(indexPath, ids);
					};
				};
			})
		]);
	};
	

	public async create (data: Partial<T>) {
		for (let key in this.schema as any as T) {

			if (key === "id") {
				data[key] = (this.schema[key].default as () => any)();
				continue;
			};

			if (this.schema[key].required && !data[key]) {
				if (!this.schema[key].default)
					throw new Error("Required value not supplied and no default defined!");
				if (typeof this.schema[key].default === "function") {
					data[key] = await (this.schema[key].default as () => any)();
				} else {
					data[key] = (this.schema[key].default as T[typeof key]);
				};
			};

			if (this.schema[key].unique)
				if (!!await this.findOne(Object.fromEntries([ [key, data[key]] ]) as any as Partial<T>))
					throw new Error("Schema requires unique value, but query found collision");
		};

		return data as T;
	};
};