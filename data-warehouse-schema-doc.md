# Data Warehouse Schema Documentation

---

## Dimension Tables

### dim_company
| Source | Target | Description |
|--------|--------|-------------|
| - | `sk_company_id` | PK, Surrogate key (generated) |
| `company.object_id` | `nk_company_id` | NK, Natural key from source (format: c:xxx) |
| `company.address1 + address2` | `full_address` | Concatenated address field |
| `company.region` | `region` | Office region |
| `company.city` | `city` | Office city |
| `company.country_code` | `country_code` | Office country code |
| `company.created_at` | `created_at` | Record creation timestamp |
| `company.updated_at` | `updated_at` | Record update timestamp |

### dim_people
| Source | Target | Description |
|--------|--------|-------------|
| - | `sk_people_id` | PK, Surrogate key (generated) |
| `people.object_id` | `nk_people_id` | NK, Natural key from source (format: p:xxx) |
| `people.first_name + last_name` | `full_name` | Concatenated full name |
| `people.created_at` | `created_at` | Record creation timestamp |
| `people.updated_at` | `updated_at` | Record update timestamp |

### bridge_company_people
| Source | Target | Description |
|--------|--------|-------------|
| - | `sk_company_people_id` | PK, Surrogate key (generated) |
| - | `sk_company_id` | FK to `dim_company.sk_company_id` |
| - | `sk_people_id` | FK to `dim_people.sk_people_id` |
| `relationship.title` | `title` | Person's title/role |
| `people.affiliation_name` | `affiliation_name` | Organization affiliation |
| `relationship.is_past` | `is_past` | Boolean for past relationship |
| `relationship.start_at` | `relationship_start_at` | Relationship start date |
| `relationship.end_at` | `relationship_end_at` | Relationship end date |
| `relationship.created_at` | `created_at` | Record creation timestamp |
| `relationship.updated_at` | `updated_at` | Record update timestamp |

### dim_fund
| Source | Target | Description |
|--------|--------|-------------|
| - | `sk_fund_id` | PK, Surrogate key (generated) |
| `funds.object_id` | `nk_fund_id` | NK, Natural key from source (format: f:xxx) |
| `funds.name` | `fund_name` | Name of the fund |
| `funds.raised_amount` (converted) | `raised_amount_usd` | Fund size in USD |
| `funds.funded_at` | `funded_at` | Date when fund was raised |
| `funds.description` | `fund_description` | Fund description |
| `funds.created_at` | `created_at` | Record creation timestamp |
| `funds.updated_at` | `updated_at` | Record update timestamp |

### dim_date
| Source | Target | Description |
|--------|--------|-------------|
| - | All columns | Date dimension (generated) |

---

## Fact Tables

### fct_investing
| Source | Target | Description |
|--------|--------|-------------|
| - | `sk_investment_id` | PK, Surrogate key (generated) |
| `investments.investment_id` | `dd_investment_id` | DD, Degenerate dimension from source |
| - | `sk_company_id` | FK to `dim_company.sk_company_id` |
| - | `sk_fund_id` | FK to `dim_fund.sk_fund_id` |
| - | `sk_date_id` | FK to `dim_date.sk_date_id` |
| `funding_rounds.funding_round_id` | `funding_round_id` | Funding round identifier |
| `funding_rounds.funding_round_type` | `funding_round_type` | Type of funding round |
| `funding_rounds.participants` (count) | `num_of_participant` | Number of participants in round |
| `funding_rounds.valuation_amount_usd` | `valuation_amount_usd` | Valuation amount in USD |
| `funding_rounds.raised_amount_usd` | `raised_amount_usd` | Total raised in USD |
| `funding_rounds.pre_money_valuation_usd` | `pre_money_valuation_usd` | Pre-money valuation in USD |
| `funding_rounds.post_money_valuation_usd` | `post_money_valuation_usd` | Post-money valuation in USD |
| `funding_rounds.created_at` | `created_at` | Record creation timestamp |
| `funding_rounds.updated_at` | `updated_at` | Record update timestamp |

### fct_ipos
| Source | Target | Description |
|--------|--------|-------------|
| - | `sk_ipo_id` | PK, Surrogate key (generated) |
| `ipos.ipo_id` | `dd_ipo_id` | DD, Degenerate dimension from source |
| - | `sk_company_id` | FK to `dim_company.sk_company_id` |
| `ipos.valuation_amount` (converted) | `valuation_amount_usd` | IPO valuation in USD |
| `ipos.raised_amount` (converted) | `raised_amount_usd` | Amount raised in IPO (USD) |
| - | `public_at` | FK to `dim_date.sk_date_id` |
| `ipos.stock_symbol` | `stock_symbol` | Stock exchange symbol |
| `ipos.source_description` | `description` | IPO description |
| `ipos.created_at` | `created_at` | Record creation timestamp |
| `ipos.updated_at` | `updated_at` | Record update timestamp |

### fct_acquisition
| Source | Target | Description |
|--------|--------|-------------|
| - | `sk_acquisition_id` | PK, Surrogate key (generated) |
| `acquisition.acquisition_id` | `dd_acquisition_id` | DD, Degenerate dimension from source |
| - | `sk_acquiring_company_id` | FK to `dim_company.sk_company_id` |
| - | `sk_acquired_company_id` | FK to `dim_company.sk_company_id` |
| `acquisition.term_code` | `term_code` | Acquisition term code |
| `acquisition.price_amount` (converted) | `price_amount_usd` | Acquisition price in USD |
| - | `acquired_at` | FK to `dim_date.sk_date_id` |
| `acquisition.source_description` | `description` | Acquisition description |
| `acquisition.created_at` | `created_at` | Record creation timestamp |
| `acquisition.updated_at` | `updated_at` | Record update timestamp |

---

## Key Abbreviations
- **PK**: Primary Key
- **NK**: Natural Key
- **FK**: Foreign Key
- **DD**: Degenerate Dimension
