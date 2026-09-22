<!--
Thanks for contributing! Please read CONTRIBUTING.md before opening a pull request.
Keep each pull request to one logical change. Bugs that also happen on Apache Kafka itself
belong upstream.
-->

### What changes and why

<!-- Link the issue or discussion this addresses (for example, "Fixes #123").
     If this change needs a LIP, link it here. -->

### Compatibility

- [ ] No change to what Kafka clients observe on diskless topics, to `ursa.*` configuration keys, to the Kafka and Lakestream contract (stream properties, WAL payload), to how the controller manages diskless topics, or to classic-topic behavior
- [ ] Changes one of the above. The change is described in *What changes and why*, with its LIP if one is required.

### How I tested it

<!-- The tests you added or changed, and the commands you ran. Say whether you ran the diskless
     integration tests (-Pkafka.ci.isolated.tests=only). -->

### AI assistance

<!-- Required when an AI tool generated or substantially rewrote code, tests, documentation or a design in
     this pull request. Autocomplete, spelling and grammar fixes, formatting and mechanical renames don't
     count. See AI_POLICY.md. Choose one: -->

- [ ] No AI assistance
- [ ] AI-assisted. Tool(s): ___ . What it did: ___ . How I verified the result: ___ .

### Checklist

- [ ] Every commit is signed off (`git commit -s`), as described in CONTRIBUTING.md
- [ ] Commits with meaningful AI assistance carry an `Assisted-by:` (or `Co-authored-by:`) trailer
- [ ] `spotlessCheck`, `rat`, and the tests for the affected modules pass locally (the tests also run checkstyle and SpotBugs)
- [ ] `LICENSE-binary` is updated and `committer-tools/verify_license.py` passes, if dependencies changed
- [ ] Documentation is updated if behavior or a contract changed
