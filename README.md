# gnome-market-data

Multi-module Maven project containing the core data types and Lambda functions for the Gnome Market Data pipeline.

## Modules

- **gnome-market-data-core** — Shared data types and utilities, published to GitHub Packages on each release.
- **gnome-market-data-lambdas** — Lambda function implementations (Merger, Transformer, Gap Detector, Inventory Processor). Built from source during AWS deployment — not published to Maven.

## CI/CD Flow

```
push to main
  → bump-and-deploy workflow
      → mvn release:prepare release:perform
      → publishes gnome-market-data-core to GitHub Packages
      → pushes 2 commits ("prepare release X.Y.Z" + "next dev iteration") + tag to main
  → tag push triggers push-release workflow
      → force-pushes tagged commit to release branch
  → AWS CodePipeline detects change on release branch
      → builds Docker image with mvn clean package from source
      → deploys Lambda functions to Dev (auto) and Prod (manual approval)
```

### Why the `release` branch?

The maven-release-plugin creates two commits per release. Pointing the CodePipeline at `main` would trigger it on every intermediate commit. The `release` branch only updates once per release (when a tag is pushed), so the pipeline runs exactly once per version bump.

## Local Development

```bash
# Build everything
mvn clean package

# Build only lambdas (and its dependency on core)
mvn clean package -pl gnome-market-data-lambdas -am
```

## Manual Release

If you need to release without pushing to `main`:

```bash
mvn release:prepare release:perform -s settings.xml
```

This will bump the version, publish `gnome-market-data-core` to GitHub Packages, and push commits + tag to `main`. The tag push will then trigger the `push-release` workflow and update the `release` branch, triggering the AWS pipeline.
