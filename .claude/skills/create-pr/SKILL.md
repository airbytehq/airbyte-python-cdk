---
description: Creates a GitHub pull request with a generated description by analyzing the current branch diff against main. Use when the user wants to open a PR.
---

# Create Pull Request

Create a GitHub pull request for the current feature branch with an auto-generated description.

## Instructions

1. **Check the current branch:**
   ```bash
   git branch --show-current
   ```
   If on `main`, inform the user to switch to a feature branch first and stop.

2. **Check for uncommitted changes:**
   ```bash
   git status --short
   ```
   If there are uncommitted changes, inform the user and ask if they want to commit first before proceeding.

3. **Push the branch to the remote:**
   ```bash
   git push -u origin HEAD
   ```

4. **Review the commit history:**
   ```bash
   git log main..HEAD --oneline
   ```

5. **Analyze the diff:**
   ```bash
   git diff main...HEAD
   ```

6. **Generate a PR title:**
   - Keep it under 70 characters
   - Use imperative mood (e.g., "Add", "Fix", "Update")
   - Summarize the core change

7. **Generate the PR description** using this template:

   ```
   ## What
   <1-3 sentences describing the overall purpose of the PR>

   ## How
   <technical explanation for how the above was achieved>

   ## Changes
   - <bullet point list of key changes>

   ## Recommended Review Order
   <ordered list of recommended review order. only include files with significant changes. avoid including tests, changelogs, documentation, and other files with trivial changes>
   ```

8. **Create the PR:**
   ```bash
   gh pr create --title "<title>" --body "$(cat <<'EOF'
   <generated description>
   EOF
   )"
   ```

9. **Return the PR URL** to the user.

## Guidelines

- In the "What" section: keep the summary concise and high-level
- Group related changes together in the bullet list
- Use clear, descriptive language
- If there are breaking changes, mention them prominently
- In "Recommended Review Order" section, only list file paths, do not include descriptions of changes to that file
- Always confirm with the user before creating the PR if there is anything ambiguous (e.g., draft vs ready, target branch other than main)
