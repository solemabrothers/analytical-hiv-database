const path = require("path");
// eslint-disable-next-line @typescript-eslint/naming-convention
const { Runner } = require("moleculer");

const runner = new Runner();

runner
	.start([
		process.argv[0],
		__filename,
		"--config",
		path.join(__dirname, "dist/moleculer.config.js"),
		"--env",
		path.join(__dirname, "dist/services"),
	])
	.catch(() => {
		process.exit(1);
	});
