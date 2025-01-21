#!/bin/bash

git fetch --tags

# Fetch the latest tag
LATEST_TAG=$(git describe --tags --abbrev=0 origin/main 2>/dev/null)

# Check if there are no tags. we want to make sure we have at least one tag to work with
# because we can't assume our default version is 0.1.0 - it already exists in the repository. ( this is still not bulletproof )
if [[ -z "$LATEST_TAG" ]]; then
  echo "Error: No tags found in the repository."
  exit 1
fi

# Define the semantic version regex -
# We want to ensures the package version follows the semver format - we will fail to publish to maven central otherwise!
semver_regex="^v?([0-9]+)\.([0-9]+)\.([0-9]+)(-[a-zA-Z0-9]+)?(\+[a-zA-Z0-9]+)?$"

# Validate the tag against the regex for the semver format
if [[ "$LATEST_TAG" =~ $semver_regex ]]; then
  echo "Current version: $LATEST_TAG"
else
  echo "Invalid semver format: $LATEST_TAG. Please tag your commit with a valid semver version - v[Major].[minor].[patch]"
  exit 1
fi


## We want to make sure the latest tag is not pointing to the current commit on the main branch
# This is to ensure we are not creating a new version for the same commit.
# We want to avoid publishing versions that doesn't include meaningful changes.

## Fetch the latest changes from the remote repository ( we want to make sure we are comparing the latest changes )
git fetch origin

## Get the commit ID of the latest tag
LATEST_TAG_COMMIT=$(git rev-list -n 1 "$LATEST_TAG")
#
## Get the latest commit ID on the main branch
LATEST_MAIN_COMMIT=$(git rev-parse "origin/main")
#
## Check if the commit IDs are the same
if [[ "$LATEST_TAG_COMMIT" == "$LATEST_MAIN_COMMIT" ]]; then
  echo "Error: The latest tag already points to the current commit on the main branch. No need to create a new version."
  exit 1
fi

## We increment the version based on the argument provided
# if it's patch then we increment the patch version
# if it's minor then we increment the minor version and reset the patch version
# if it's major then we increment the major version and reset the minor and the patch version

# Function to increment the patch version
increment_patch() {
  IFS='.' read -r major minor patch <<< "${LATEST_TAG:1}"
  patch=$((patch + 1))
  echo "v${major}.${minor}.${patch}"
}

# Function to increment the minor version
increment_minor() {
  IFS='.' read -r major minor patch <<< "${LATEST_TAG:1}"
  minor=$((minor + 1))
  patch=0
  echo "v${major}.${minor}.${patch}"
}

# Function to increment the major version
increment_major() {
  IFS='.' read -r major minor patch <<< "${LATEST_TAG:1}"
  major=$((major + 1))
  minor=0
  patch=0
  echo "v${major}.${minor}.${patch}"
}

if [ "$#" -gt 1 ]; then
  echo "Error: Only one argument is allowed (--patch, --minor, or --major)."
  exit 1
fi

# Default to --patch if no arguments are provided
# Usually, we want to increment the patch version by default and be explicit about the other versions
# We want to include new features when we increment the minor version and breaking changes when we increment the major version
action="${1:---patch}"

case "$action" in
  --patch)
    new_version=$(increment_patch)
    ;;
  --minor)
    new_version=$(increment_minor)
    ;;
  --major)
    new_version=$(increment_major)
    ;;
  *)
    echo "Error: Invalid argument. Use --patch, --minor, or --major."
    exit 1
    ;;
esac

echo "New version: $new_version"
echo "Tagging commit - $LATEST_MAIN_COMMIT - with $new_version"
git tag -a "$new_version" -m "Artifact $new_version" "$LATEST_MAIN_COMMIT"
git push origin "$new_version"