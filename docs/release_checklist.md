# Release and repository hygiene checklist

This checklist is for SweetSpot releases. The public CLI, import package, schemas, and worker command should remain `sweetspot` / `SWEETSPOT_*` unless a future release deliberately plans another breaking rename.

## Before tagging

1. Run local closeout checks:

   ```bash
   scripts/verify_release.sh
   ```

2. Confirm GitHub Actions passes on the exact commit to be tagged:
   - Python matrix
   - OpenTofu validation
   - container build/SBOM/provenance/Trivy scan

3. Verify the workflow still scans and uploads the same OCI artifact path:
   - build output: `/tmp/sweetspot-worker.oci.tar`
   - Trivy input: `/tmp/sweetspot-worker.oci.tar`
   - uploaded artifact path: `/tmp/sweetspot-worker.oci.tar`

4. Check supply-chain pins:
   - GitHub Actions are full commit SHAs, with version comments for maintainability.
   - Composite actions' nested `uses:` references resolve.
   - `requirements.lock` and `requirements-dev.lock` match the intended dependency set.
   - `infra/opentofu/.terraform.lock.hcl` is unchanged after `tofu init -lockfile=readonly`.
   - Docker base image remains digest-pinned.

5. Review docs for stale naming before public release:
   - `README.md`
   - `pyproject.toml`
   - `docs/*.md`
   - `infra/opentofu/README.md`
   - GitHub repository description/topics

6. Confirm the trust-boundary wording remains prominent:
   - trusted producers only;
   - idempotent tasks;
   - queue access implies command execution by the worker role;
   - not a sandbox for arbitrary untrusted code.

## Suggested branch protection

Configure these in GitHub after CI is green:

- require pull request review before merge;
- require status checks for all CI jobs on `main`;
- require branches to be up to date before merge;
- disallow force-pushes and deletions on `main`;
- require signed tags or protected tags for releases if available in the repository plan;
- restrict who can edit GitHub Actions workflows.

## Updating pinned GitHub Actions

When updating an action:

1. Resolve the desired tag to a commit SHA. Fetch both the tag ref and the peeled ref; annotated tags have a `^{}` peeled commit, while lightweight tags only return the tag ref:

   ```bash
   git ls-remote --tags https://github.com/OWNER/REPO.git \
     'refs/tags/vX.Y.Z' 'refs/tags/vX.Y.Z^{}'
   ```

   Use the `^{}` SHA when present; otherwise use the tag-ref SHA.

2. Use the resolved commit SHA in workflow `uses:` and keep a comment with the tag.
3. For composite actions, inspect `action.yml` / `action.yaml` at that commit and confirm nested `uses:` references resolve.
4. Push a branch and confirm CI runs before merging.

## Case studies

Cost claims should ship with:

- `examples/run_manifest.example.json`-compatible manifest;
- `docs/case_study_template.md`-compatible prose;
- timestamps and sources for price assumptions;
- clear labels for estimated vs billed costs.
