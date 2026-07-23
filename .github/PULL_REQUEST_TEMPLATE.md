<!-- Keep PRs focused: one concern each. See CONTRIBUTING.md. -->

## What & why

<!-- What does this change, and why? Explain the reasoning, not just the diff. -->

## Checklist

- [ ] `make test_unit` passes (`-race` clean)
- [ ] `go vet ./...` and `make go_lint` pass
- [ ] No secrets or `local/` contents are committed; no key value is printed or logged
- [ ] New middleware is registered in `cmd/sagegw.Build` and named/ordered in `relay/chain_order.go` (with a `mustPrecede` rule if order is load-bearing)
- [ ] New config fields are value types with a safe zero value, and actually read somewhere (not left in `cfg.Ignored`)
- [ ] New runtime-toggleable behavior is gated behind a flag added to `featureflag.DefaultFlags`
- [ ] If this fixes a bug that also exists in PATH, I said so

## Notes for reviewers

<!-- Anything load-bearing or non-obvious a reviewer should know. -->
