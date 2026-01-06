# Commit Changes

Create a git commit for the current changes. Follow these steps:

1. Run `git status` to see all changed files
2. Run `git diff` to review the actual changes
3. Analyze the changes and determine the appropriate commit type:
   - `feat`: New feature
   - `fix`: Bug fix
   - `docs`: Documentation changes
   - `style`: Code style/formatting (no logic change)
   - `refactor`: Code refactoring
   - `test`: Adding/updating tests
   - `chore`: Maintenance tasks

4. Write a clear, concise commit message in the format: `type: description`
   - Keep the description under 72 characters
   - Use imperative mood ("Add feature" not "Added feature")
   - Focus on the "why" not just the "what"

5. Stage appropriate files with `git add`
6. Create the commit

7. After committing, run `git status` to confirm the commit was successful

If there are no changes to commit, inform the user.
