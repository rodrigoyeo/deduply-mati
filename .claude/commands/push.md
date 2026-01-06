# Push to GitHub

Push the current branch to the remote repository.

1. First, run `git status` to check:
   - Current branch name
   - Whether there are unpushed commits
   - Whether the branch is tracking a remote

2. Run `git log --oneline -5` to show recent commits that will be pushed

3. Push to origin:
   - If branch is already tracking remote: `git push`
   - If new branch: `git push -u origin <branch-name>`

4. Confirm the push was successful

Remote: https://github.com/rodrigoyeo/deduply-mati.git
