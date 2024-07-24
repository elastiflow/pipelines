- [Overview](#overview)
- [Repo settings](#repo-settings)

## Overview
Use this repo as a [template repo for new Go modules](https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template)
* Please module change the `module_template` name in the `go.mod` and `example_test.go`
* Please add some description on what/how module should be used for
* Add some "honorable" notes if any
* Please add some examples to the `examples`

## Repo settings
Please don't forget to set following properties in the repo settings and drop this section right after.
* Allow "Allow squash merging" only in the "General settings"
* Check the "Always suggest updating pull request branches" option in the "General" settings
* Check the "Automatically delete head branches" option in the "General" settings
* Add `Ops` team with "Admin" permissions
* Add `Centaur` team with "Maintain" permissions
* Add `main` branch protection rules in "Branches":
  * "Require a pull request before merging"
  * "Require approvals"
  * "Dismiss stale pull request approvals when new commits are pushed"
  * "Require status checks to pass before merging"
  * "Require conversation resolution before merging"
  * "Require linear history"
  * "Do not allow bypassing the above settings"
