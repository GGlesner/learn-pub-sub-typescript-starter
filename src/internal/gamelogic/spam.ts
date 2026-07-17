import { getMaliciousLog } from "./gamelogic.js";

export async function commandSpam(
  words: string[],
  publisher: (m: string) => Promise<void> | void,
): Promise<void> {
  if (words.length < 2 || words[1] === undefined) {
    throw new Error("usage: spam <number>");
  }
  let n: number;
  n = parseInt(words[1], 10);
  if (isNaN(n)) {
    throw new Error(`usage: spam <number>\n expected a number got ${words[1]}`);
  }
  for (let i = 0; i < n; i++) {
    try {
      await publisher(getMaliciousLog());
    } catch (err) {
      console.log("Failed to publish message: ", (err as Error).message);
      continue;
    }
  }
  console.log(`Published ${n} malicious logs`);
}
