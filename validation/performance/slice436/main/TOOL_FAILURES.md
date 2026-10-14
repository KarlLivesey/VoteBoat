# Recorded tool failure

After actual original72713 terminal0 and preservation under attempt1, a test-only follow-up apply_patch failed verification because its locator expected a multiline formatted receipt-send expression while the checked source had that expression on one line. Actual tool result: `apply_patch verification failed: Failed to find expected lines in .../voteboat_engine/waits/tests.rs`. No compiler/test failure occurred in this attempt. The focused retry uses the actual formatted source. No synthetic raw compiler output is substituted for this tool-only failure.
