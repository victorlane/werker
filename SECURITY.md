# Security policy

## Supported versions

Security fixes are applied to the latest development release until a stable
release policy is established. Please upgrade to the newest available version
before reporting an issue.

## Reporting a vulnerability

Please report suspected vulnerabilities privately to the repository maintainer
using GitHub's private vulnerability reporting feature. If that feature is not
available, contact the maintainer through the email address on their GitHub
profile. Do not file a public issue or include a proof of concept that exposes
credentials or production data.

Include the affected version, deployment context, reproduction steps, impact,
and any suggested mitigation. You should receive an acknowledgement within
seven days. We will assess the report, coordinate a fix and disclosure, and
credit reporters unless they prefer to remain anonymous.

## Deployment notes

Werker processes application task arguments, return values, and exception
tracebacks in PostgreSQL. Restrict database and Django admin access, avoid
placing secrets in task arguments or return values, apply database backups and
encryption policies appropriate to that data, and run workers with the least
privilege necessary.