# DBMasker 简介

DBMasker 是一个针对主流数据库系统的 Java 开源项目，旨在提供统一且安全的访问接口。它支持多种数据库，包括主流的关系型数据库（如 MySQL, Oracle, SQLite, PostgreSQL）以及国产数据库（如达梦、人大金仓、南大通用），还涵盖大数据仓库（如 HBase、Elasticsearch、Hive）。DBMasker 提供了易用的 Java API，包括元数据接口、数据接口、SQL生成接口和敏感数据处理接口，致力于为不同类型的用户带来一致、安全的数据库访问体验，使数据库管理、开发、安全和分析等工作更加高效便捷。

DBMasker 为数据库管理员、开发人员、数据安全专家、数据科学家和分析师等用户提供了便利，帮助他们轻松访问和管理多种数据库系统，实现敏感数据保护和隐私保护，同时支持高效的数据分析和挖掘。

# 适配数据库

| 数据库名            | 版本    | 支持情况     |
|-----------------|-------|----------|
| sqlite          | v3    | &#x2713; |
| 达梦（DM）          | v8    | &#x2713; |
| 人大金仓（KingBase）  | v8    | &#x2713; |
| 南大通用（GBase）     | 8a    | &#x2713; |
| 南大通用（GBase）     | 8s    | &#x2713; |
| 南大通用（GBase）     | 8t    | &#x2713; |
| 神通 (Oscar)      | v7    | &#x2713; |
| hive            | v2    | &#x2713; |
| es (sql-jdbc)   | v8    | &#x2713; |
| hbase (Phoenix) | v1-v4 | &#x2713; | 
| mysql           | v8    | &#x2713; |
| mariodb         | v10   | &#x2713; |
| oceanbase       | v2    | &#x2713; |
| mssql           | v13   | &#x2713; |
| oracle          | v11   | &#x2713; |
| postgreSQL      | v9    | &#x2713; |

# 用户画像

数据库管理员（DBA）和开发人员：这些用户需要访问和管理多种数据库系统，包括关系型数据库和大数据仓库。他们可以利用 DBMasker 提供的元数据接口、数据接口和SQL生成接口来方便地执行常见的数据库操作，如获取表信息、执行 SQL 查询和更新等。

数据安全和合规专家：这些用户关注数据的安全性和隐私保护，他们需要确保敏感数据不会被泄露。DBMasker 提供的敏感数据处理接口可以帮助他们实现数据的脱敏和保护，如数据掩码、截取脱敏、泛化等。

数据科学家和分析师：这些用户需要对数据进行深入分析，但同时也需要保护数据隐私。他们可以利用 DBMasker 提供的敏感数据处理接口对数据进行脱敏处理，确保在数据分析过程中不泄露敏感信息。


# 应用场景

数据库管理和开发：DBMasker 可用于开发数据库客户端，快速访问和操作多种数据库系统，提高工作效率。例如，他们可以使用 DBMasker 提供的元数据接口、数据接口和SQL生成接口来执行常见的数据库操作，如获取表信息、执行 SQL 查询和更新等。

数据安全和隐私保护：DBMasker 可用于开发数据库安全保护系统，帮助企业在满足数据安全和合规要求的同时，实现对敏感数据的保护。例如，金融、医疗等行业在处理客户数据时，可以使用 DBMasker 提供的敏感数据处理接口对数据进行脱敏处理，确保数据的隐私不会被泄露。

数据分析和挖掘：DBMasker 可助于开发数据分析和挖掘工具，帮助数据科学家和分析师在保护数据隐私的同时，对数据进行深入分析。例如，在进行用户行为分析时，可以使用 DBMasker 提供的敏感数据处理接口对用户的个人信息进行脱敏处理，确保在数据分析过程中不泄露敏感信息。

数据治理和审计：DBMasker 可助于开发数据治理和审计工具，可以帮助企业在进行数据治理和审计时，实现对敏感数据的保护。例如，在对数据库系统进行审计时，可以使用 DBMasker 提供的敏感数据处理接口对审计数据进行脱敏处理，确保审计过程中不泄露敏感信息。

# [API 概览](API_OVERVIEW.md)

# Getting Started

参考 Tutorial

# Contributing
We welcome contributions to DBMasker! Please follow the Contributing Guidelines to get started.

# License
DBMasker is released under the Apache License 2.0 License.

