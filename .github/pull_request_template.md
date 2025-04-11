### Checklist before merging:
*Required for all PRs.*

When creating a PR these are items to keep in mind that cannot be checked by GitHub actions:

- [ ] Documentation:
  - [ ] Does developer documentation (code comments, readme, etc.) need to be added or updated?
  - [ ] Does the user documentation need to be expanded or updated for this change?
- [ ] Testing:
  - [ ] Does this functionality require changing or adding new unit tests?
  - [ ] Does this functionality require changing or adding new integration tests?
- [ ] Git Hygiene: 
  - [ ] Do commits adhere to [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)?
  - [ ] Is the commit history squashed down reasonably?

For more details refer to the [Go coding standards](https://github.com/ThinkParQ/beegfs-go/wiki/Getting-Started-with-Go#coding-standards) and the [pull request process](https://github.com/ThinkParQ/beegfs-go/wiki/Pull-Requests).

### Related Issue(s)
*Required when applicable.*
<!-- Link the PR to issues(s) using keywords:
* Reference: https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/linking-a-pull-request-to-an-issue
-->

### What does this PR do / why do we need it?
*Required for all PRs.*
<!--Questions that may be helpful filling out this section:

* What do we gain/lose with this PR?
-->

### Where should the reviewer(s) start reviewing this? 
*Only required for larger PRs when this may not be immediately obvious.*
<!-- Questions that may be helpful filling out this section:

* Where should someone start (file/line) to begin reviewing the new/updated functionality in this
  PR? 
* Is there a logical progression the reviewer can follow to navigate their way through the changes
  (i.e., main.go -> api.go -> db.go)?
-->

### Are there any specific topics we should discuss before merging?
*Not required.*
<!-- Questions that may be helpful filling out this section:

* Are there potential tradeoffs in the implementation?
-->

### What are the next steps after this PR? 
*Not required.*
<!-- Questions that may be helpful filling out this section:

* Is further work planned in this area after this PR is merged? 
  * If so, what issue(s) and/or PRs is it tracked in? 
-->