import { mkdir, readJsonFile, writeJsonFile } from "@terracedb/sandbox/fs";
import { camelCase } from "lodash";
import { z } from "zod";
import { listNotes, addComment } from "terrace:host/notes";

const config = await readJsonFile("/workspace/inbox.json");
const noteSchema = z.object({
  id: z.string(),
  title: z.string(),
  status: z.string(),
  comments: z.string(),
});
const notes = (await listNotes()).map((note) =>
  noteSchema.parse({
    ...note,
    comments: JSON.stringify(note.comments),
  }),
).map((note) => ({
  ...note,
  comments: JSON.parse(note.comments),
}));
const openNotes = notes.filter((note) => note.status === "open");
const summary = {
  project: config.project,
  openCount: openNotes.length,
  slugs: openNotes.map((note) => camelCase(note.title)),
};

await mkdir("/workspace/generated");
await writeJsonFile("/workspace/generated/triage-summary.json", summary);

const updatedNote = await addComment({
  noteId: openNotes[0].id,
  author: config.reviewer,
  body: `Reviewed ${config.project} with ${summary.openCount} open notes`,
});

export default {
  summary,
  updatedNote,
};
