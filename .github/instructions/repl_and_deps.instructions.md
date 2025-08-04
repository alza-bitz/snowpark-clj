---
applyTo: '**'
---

- A Calva REPL should already started, based on a skeleton deps.edn file. However, if you find the Calva REPL is not started, please ask me to start it, and then prefer the REPL to the command line in all cases.
- Use the Calva backseat driver tools at each step, for example the evaluate, calva-output and balance-brackets tools to check the code you write with the Calva REPL, check namespaces load without error and check the tests pass.
- If you need to add deps, use add-lib by first running `(require '[clojure.repl.deps :refer :all])`. If it succeeds update the deps.edn file and then tell me to restart the Calva REPL before continuing! If add-lib fails, just add deps directly to deps.edn. If you don't know the correct version of a dep to use, I can fix it manually and let you know when it's done so that you can continue.
- Use tools.namespace to reload namespaces if needed.
- When you comment code using `comment`, use the Calva backseat driver balance-brackets tool to ensure the comment is balanced.
- Before you tell me you're done with my prompts, use the Calva REPL to check namespaces load without error and check the tests pass.
