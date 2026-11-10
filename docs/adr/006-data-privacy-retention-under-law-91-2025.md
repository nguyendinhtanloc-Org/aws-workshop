# ADR 006: Data Privacy, Masking and Retention under Law 91/2025/QH15

- **Status:** Accepted
- **Date:** 2026-11-10

## Context
Under Vietnamese Law on Personal Data Protection No. 91/2025/QH15 and Decree No. 356/2025/ND-CP (effective January 1, 2026), shipping order data contains Personally Identifiable Information (PII: recipient name, phone number, physical address).
Simultaneously, the Vietnam Accounting Law and Postal Law mandate retaining delivery contracts and accounting records for statutory audit periods (1 to 5 years).

## Decision
1. **Statutory Retention Basis:** Shipping orders are classified as statutory service contracts and accounting transaction records (Legal Obligation exemption). They are retained under strict security controls for the legal retention period.
2. **Column-Level Masking View:** Expose an Athena SQL View for BI and data analysts where PII fields are masked:
   `CONCAT(SUBSTR(to_phone, 1, 3), '*****', SUBSTR(to_phone, 9, 2))`.
3. **Row-Level Redaction:** When legitimate data subject deletion requests are granted after the statutory dispute period, perform row-level redaction on the serving layer (`silver.orders` and DynamoDB) using Athena `UPDATE ... SET to_name = 'REDACTED', to_phone = 'REDACTED'`.
4. **Lifecycle Expiry:** Raw data in S3 expires automatically via S3 Lifecycle policies (30 days S3 Standard, 1 year Glacier Archive).

## Consequences
- **Positive:**
  - Full compliance with Law 91/2025/QH15 without violating legal accounting retention mandates.
  - Avoids overly complex per-customer crypto-shredding key management.
- **Negative:**
  - Analysts cannot access raw phone numbers without formal compliance approval.

