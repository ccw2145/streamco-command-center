-- OWNER: Stream C (GenAI+Governance). Governance functions. Run by notebooks/07 (idempotent, before
-- classification) and notebooks/26 (before 02_apply.sql) through cc_common.sqlfile.
-- Tokens: {catalog}, {schema}. Statements end with ';' at end of line; function bodies keep ';' off line ends.
--
-- Members of the account group `cc_privileged` see clear values through the masks and the row filter.
-- fn_redact_pii is the exception: it is the redactor applied BEFORE any text reaches the model
-- (notebooks/07), so it redacts for everyone, including privileged users. mask_transcript is the
-- privileged-aware column mask built on it.
-- The PII patterns match the four slots in seed/phrase_banks (phone, email, card last-4, single-line address);
-- full card numbers are never generated. Replacement tokens match the fixture's masking example.

CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_redact_pii(text STRING)
RETURNS STRING
COMMENT 'Replaces emails, phone numbers, card last-4 digits and street addresses with [EMAIL], [PHONE], [CARD] and [ADDRESS]. Applied to every support transcript before it is sent to a model, for every user.'
RETURN
  regexp_replace(
    regexp_replace(
      regexp_replace(
        regexp_replace(
          text,
          '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}', '[EMAIL]'),
        '(?i)((?:card|ending|ends|last four|last 4)[^0-9\\n]{0,15})[0-9]{4}\\b', '$1[CARD]'),
      '(\\+?1[ .-]?)?\\(?[0-9]{3}\\)?[ .-]?[0-9]{3}[ .-]?[0-9]{4}\\b', '[PHONE]'),
    '\\b[0-9]{1,5} [A-Z][A-Za-z]*( [A-Z][A-Za-z]*)* (Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct|Way|Place|Pl)\\b\\.?(, [A-Z][A-Za-z]*( [A-Z][A-Za-z]*)*)?(, [A-Z]{2})?( [0-9]{5})?',
    '[ADDRESS]');

CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_count_redactions(text STRING)
RETURNS INT
COMMENT 'Number of redaction tokens ([EMAIL], [PHONE], [CARD], [ADDRESS]) in already-redacted text.'
RETURN size(regexp_extract_all(coalesce(text, ''), '\\[(EMAIL|PHONE|CARD|ADDRESS)\\]', 0));

CREATE OR REPLACE FUNCTION {catalog}.{schema}.rf_exclude_kids(profile_type STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: hides kids profiles from everyone except members of cc_privileged, so kids profiles can never reach a targeting query.'
RETURN is_account_group_member('cc_privileged') OR coalesce(profile_type, '') <> 'kids';

CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_email(email STRING)
RETURNS STRING
COMMENT 'Column mask: keeps the first character and the domain (j***@example.com) unless the reader is in cc_privileged.'
RETURN CASE
  WHEN is_account_group_member('cc_privileged') THEN email
  WHEN email IS NULL THEN NULL
  ELSE regexp_replace(email, '^(.)[^@]*@', '$1***@')
END;

CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_phone(phone STRING)
RETURNS STRING
COMMENT 'Column mask: shows only the last two digits ((***) ***-**12) unless the reader is in cc_privileged.'
RETURN CASE
  WHEN is_account_group_member('cc_privileged') THEN phone
  WHEN phone IS NULL THEN NULL
  ELSE concat('(***) ***-**', right(regexp_replace(phone, '[^0-9]', ''), 2))
END;

CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_address(postal_address STRING)
RETURNS STRING
COMMENT 'Column mask: replaces the street address with [ADDRESS] unless the reader is in cc_privileged.'
RETURN CASE
  WHEN is_account_group_member('cc_privileged') THEN postal_address
  WHEN postal_address IS NULL THEN NULL
  ELSE '[ADDRESS]'
END;

CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_transcript(transcript_text STRING)
RETURNS STRING
COMMENT 'Column mask for support transcripts: phone, email, card last-4 and address redacted unless the reader is in cc_privileged.'
RETURN CASE
  WHEN is_account_group_member('cc_privileged') THEN transcript_text
  ELSE {catalog}.{schema}.fn_redact_pii(transcript_text)
END;
