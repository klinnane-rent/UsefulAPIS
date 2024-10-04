#!/bin/bash

# Set your branch name
BRANCH="main"

# Find files larger than 100MB
echo "Identifying files larger than 100MB..."
LARGE_FILES=$(find . -type f -size +100M)

if [ -z "$LARGE_FILES" ]; then
  echo "No large files found (greater than 100MB)."
else
  echo "The following large files will be removed from Git tracking:"
  echo "$LARGE_FILES"

  # Loop through and remove the large files from Git's index (keep them locally)
  while IFS= read -r FILE; do
    echo "Removing $FILE from Git tracking..."
    git rm --cached "$FILE"
  done <<< "$LARGE_FILES"

  # Commit the removal of large files
  echo "Committing changes..."
  git commit -m "Removed large files exceeding 100MB from Git history"

  # Force push the changes to the remote repository
  echo "Force pushing changes to remote..."
  git push --force origin "$BRANCH"

  echo "Large files have been removed from Git and changes pushed to the repository."
fi
