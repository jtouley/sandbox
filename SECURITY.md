<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Security policy](#security-policy)
  - [Reporting a vulnerability](#reporting-a-vulnerability)
  - [Scope](#scope)
  - [Supported versions](#supported-versions)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Security policy

## Reporting a vulnerability

Please **don't** open a public issue. Use GitHub's private reporting instead:
**Security → Report a vulnerability** on this repository
(<https://github.com/jtouley/sandbox/security/advisories/new>).

Include what you found, how to reproduce it, and the impact you expect. You'll get an
acknowledgement within 7 days and a status update within 30.

## Scope

Most of this repository is learning material. The code that handles credentials and
personal data is held to a higher bar:

- `labs/day07_comment_guard/`: stores an Instagram access token and comment data.
  Particularly in scope: token leakage (into files, logs, URLs, or the data lake), bypasses
  of the landing-file integrity checks, and dependency vulnerabilities.

## Supported versions

Only the latest commit on `main` is supported.
