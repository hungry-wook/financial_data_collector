# Windows UTF-8 Editing

Use these patterns only when `apply_patch` has already failed on a smaller patch.

## Exact Text Replacement

```powershell
$path = "path\\to\\file.txt"
$content = [System.IO.File]::ReadAllText($path, [System.Text.Encoding]::UTF8)
$old = @"
old text
"@
$new = @"
new text
"@
if (-not $content.Contains($old)) {
    throw "Target text not found."
}
$content = $content.Replace($old, $new)
[System.IO.File]::WriteAllText($path, $content, [System.Text.UTF8Encoding]::new($false))
```

## Single-Line Replacement

```powershell
$path = "path\\to\\file.txt"
$content = [System.IO.File]::ReadAllText($path, [System.Text.Encoding]::UTF8)
$updated = $content -replace [regex]::Escape("old line"), "new line"
if ($updated -eq $content) {
    throw "Replacement produced no change."
}
[System.IO.File]::WriteAllText($path, $updated, [System.Text.UTF8Encoding]::new($false))
```

## Verification

- Re-read the file after writing.
- Inspect `git diff -- path/to/file`.
- Stop if the replacement count is unclear or multiple matches would be risky.