---
name: windows-patch-fallback
description: Safe file editing workflow for Windows when `apply_patch` fails repeatedly or the model starts proposing full-file shell overwrites. Use for small to medium source edits, UTF-8 preservation, PowerShell fallback decisions, and minimizing unrelated diffs in Windows repositories.
---

# Windows Patch Fallback

Prefer `apply_patch`. Treat shell-based editing as a constrained fallback, not the default path.

Read [references/windows_utf8_editing.md](references/windows_utf8_editing.md) when you need concrete PowerShell UTF-8 write patterns.

## Workflow

1. Inspect the target file and isolate the smallest possible change.
2. Retry `apply_patch` with one file and one logical hunk at a time.
3. Reduce patch context if the patch is large or touches unrelated lines.
4. If `apply_patch` still fails, use PowerShell to perform a targeted UTF-8 edit instead of rewriting the full file.
5. Re-read the file or diff after editing to confirm that only intended lines changed.

## `apply_patch` Rules

- Use raw patch input only. Do not wrap the patch in JSON.
- Keep each patch narrow. Prefer one file per patch and one to three hunks.
- Preserve surrounding context lines exactly as they appear in the file.
- Avoid full-file replacement when only a few lines need to change.
- If line-ending mismatch appears likely, re-read the file first and patch against the exact current text.

Minimal pattern:

```text
*** Begin Patch
*** Update File: path/to/file.ext
@@
-old line
+new line
*** End Patch
```

## PowerShell Fallback Rules

- Use fallback only after at least one smaller `apply_patch` retry.
- Modify specific substrings or blocks in memory. Do not regenerate the whole file unless the task is explicitly a full rewrite.
- Write UTF-8 explicitly and avoid accidental encoding changes.
- Preserve the rest of the file byte-for-byte as much as practical.
- After fallback, inspect the diff immediately.

Preferred fallback sequence:

1. Load the file as UTF-8 text.
2. Replace only the exact target text.
3. Write with `System.IO.File.WriteAllText` and `UTF8Encoding($false)`.
4. Re-open or diff the file.

## What To Avoid

- Saying that `apply_patch` is broken without first trying a smaller patch.
- Using shell overwrite as the first answer on Windows.
- Using broad regex replacements when the target text is ambiguous.
- Touching unrelated formatting, ordering, or whitespace without a reason.
- Hiding the fallback decision. State that you are using a targeted UTF-8 shell edit because `apply_patch` failed after retry.

## Response Pattern

When you switch to fallback, say it plainly:

```text
`apply_patch` failed again on this file, so I am using a targeted PowerShell UTF-8 edit for the exact block only and will verify the resulting diff.
```