import { fork } from "child_process";

function restart() {
  let app = fork("index.js");
  app.on("close", () => {
    console.log(
      "\x1b[36m",
      "\nScript ended. Next start in 12 minutes...\n",
      "\x1b[0m"
    );
    setTimeout(() => {
      restart();
    }, 12 * 60 * 1000);
  });
}
restart();
