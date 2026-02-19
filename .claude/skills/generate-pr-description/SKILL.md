---
description: Generates a PR description by analyzing the current branch diff against main. Use when preparing a pull request.
---

# PR Description Generator

Generate a pull request description by analyzing the current feature branch.

## Instructions

1. **Check the current branch:**
   ```bash
   git branch --show-current
   ```
   If on `main`, inform the user to switch to a feature branch first.

2. **Review the commit history:**
   ```bash
   git log main..HEAD --oneline
   ```

3. **Analyze the diff:**
   ```bash
   git diff main...HEAD
   ```

4. **Generate the PR description** using this template:

```markdown
## What
<1-3 sentences describing the overall purpose of the PR>

## How
<technical explanation for how the above was achieved>

## Changes
- <bullet point list of key changes>

## Recommended Review Order
<ordered list of recommended review order. only include files with significant changes. avoid including tests, changelogs, documentation, and other files with trivial changes>
```

## Guidelines

- In the "what" section: keep the summary concise and high-level
- Group related changes together in the bullet list
- Use clear, descriptive language
- If there are breaking changes, mention them prominently
- In "Recommend Review Order" section, only list file path, do not include changes to that file.
- Return the markdown PR description wrapped in a codeblock.
