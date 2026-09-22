# AI-assisted contributions

Many developers use AI coding tools, and so do the people who build Ursa for Apache Kafka (UFK).
You're welcome to use them here. This page explains what we ask in return, so that reviewers can trust
what they read and spend their time on your change rather than on working out how it was made.

It applies to everything you contribute: code, tests, documentation, commit messages, pull requests,
issues, discussions and review comments.

## The short version

1. **You are the author.** You understand every line you submit, you have tested it, and you can
   explain it.
2. **Disclose meaningful AI help** in the pull request and with an `Assisted-by:` commit trailer.
3. **Only a human signs off.** An AI tool never adds `Signed-off-by`.
4. **A human approves every outward action.** Agents don't push, open pull requests or issues, or
   comment on their own.
5. **Words you post are yours.** Edit AI-drafted text before you post it.

## 1. You are the author

Using a tool doesn't change who is responsible for a contribution. Before you ask for review:

- Read and understand every change. You should be able to explain why it's correct, and why it's
  written the way it is, without going back to the tool.
- Build and test it yourself with the checks in [CONTRIBUTING.md](CONTRIBUTING.md#build-and-test).
  A reviewer should never be the first person to run your code.
- Keep the change focused. Remove unrelated edits, speculative abstractions and boilerplate the
  change doesn't need.
- Take responsibility for licensing. Use tools whose terms let you contribute their output under the
  Apache License 2.0, and don't submit output that reproduces third-party code you don't have the
  right to contribute.

Reviewers will ask you about your change. If you can't answer their questions, it isn't ready to
merge.

## 2. Disclose meaningful AI help

**What counts.** Disclose when an AI tool generated or substantially rewrote code, tests,
documentation or a design you are submitting. You don't need to disclose autocompletion of a few
tokens, spelling and grammar fixes, formatting, or mechanical renames.

**Where.**

- **In the pull request:** fill in the *AI assistance* section of the template. Say which tool you
  used, what it did, and how you checked the result.
- **In the commit:** add a trailer that names the tool. We recommend `Assisted-by:`.
  `Co-authored-by:` is also accepted.

```text
Fence stream lifecycle operations

Explain what changed and why.

Assisted-by: Claude Code
Signed-off-by: Jane Doe <jane@example.com>
```

Adding the model is optional (`Assisted-by: <tool> (<model>)`) and can help reviewers.

This repository configures Claude Code to add `Assisted-by: Claude Code` to the commits it writes
(see [`.claude/settings.json`](.claude/settings.json)). With other tools, add the trailer yourself.

Changes you send upstream to Apache Kafka follow Apache Kafka's own
[guidelines](https://github.com/apache/kafka/blob/trunk/CONTRIBUTING.md#ai-generated-contributions),
which ask for a `Co-Authored-By:`, `Generated-by:` or similar trailer.

## 3. Only a human signs off

Every commit needs a [Developer Certificate of Origin](https://developercertificate.org/) sign-off
(see [CONTRIBUTING.md](CONTRIBUTING.md#sign-your-commits-dco)). A sign-off states that *you* have
the right to submit the work. A tool can't make that statement, so:

- AI tools and agents never add a `Signed-off-by` line.
- You add it yourself after you've reviewed the change: `git commit -s`, `git commit --amend -s
  --no-edit` for the last commit, or `git rebase --signoff origin/4.3-ursa` for a series.
- Your sign-off covers the whole commit, including the parts a tool helped with.

## 4. A human approves every outward action

Agents can edit, build and test in your local checkout. Anything that leaves your machine is a
decision for a person:

- An agent may push, open or update a pull request or issue, or post a comment, only when you've
  approved that specific action. Asking an agent to start a task isn't permission to publish the
  result.
- Every pull request, issue and comment needs a human who has reviewed the work and answers for it.

Maintainers use AI tools on this repository too. For example, Claude Code answers `@claude` mentions
and can review pull requests. Only maintainers can trigger these tools, and a maintainer's request
approves that one run. Their output is advisory, and a maintainer approves every merge. Commits that
a tool pushes carry no sign-off, so the maintainer who takes them adds it.

## 5. Words you post are yours

Issues, pull request descriptions, discussion posts and review replies are how we talk to each
other, so we ask that they come from you:

- If a tool drafted the text, edit it until it says what you mean, concisely. Delete anything you
  can't vouch for.
- Answer review comments yourself. Don't paste a tool's reply to a reviewer's question.
- If you use a tool to review someone else's pull request, check each finding before you post it.

## For reviewers

The same rules apply to maintainers. AI review tools are a second pair of eyes, not an approval. A
maintainer reads and approves every change that merges.

Triggering a tool on a pull request runs it against that pull request's code: it builds what the
branch contains, and it follows the agent instructions the branch carries. Only trigger AI tools on
pull requests whose changes you have read, including build files and the `AGENTS.md` files.

## Why we ask

Review is the scarcest resource in an open source project. Disclosure tells reviewers where to look
harder. Accountability keeps the cost of a change with its author rather than its reviewers. A human
sign-off keeps the project's licensing clean. None of this is about whether you used a tool. It's
about who stands behind the result.

## Where this comes from

We borrowed freely from projects that worked through these questions before us:

- The Linux kernel's guidance on [AI coding assistants](https://docs.kernel.org/process/coding-assistants.html)
  and [tool-generated content](https://docs.kernel.org/process/generated-content.html)
- The [LLVM AI tool policy](https://llvm.org/docs/AIToolPolicy.html)
- The [Fedora AI-assisted contributions policy](https://docs.fedoraproject.org/en-US/council/policy/ai-contribution-policy/)
- The [OpenJS Foundation AI coding assistants policy](https://ai-coding-assistants-policy.openjsf.org/)
- The [ASF generative tooling guidance](https://www.apache.org/legal/generative-tooling.html)
- Apache [Kafka](https://github.com/apache/kafka/blob/trunk/CONTRIBUTING.md#ai-generated-contributions),
  [Iceberg](https://iceberg.apache.org/contribute/#guidelines-for-ai-assisted-contributions),
  [Flink](https://github.com/apache/flink/blob/HEAD/AGENTS.md),
  [Airflow](https://github.com/apache/airflow/blob/HEAD/contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions)
  and [Pulsar](https://github.com/apache/pulsar/blob/HEAD/AGENTS.md)
- [Ghostty](https://github.com/ghostty-org/ghostty/blob/HEAD/AI_POLICY.md)

Changes to this policy go through a pull request, like any other change.
