# D1 moved-test ledger

Source inventory: `crates/collaboration-central-core` at M3 commit `9cc19dd2`. All **163 test declarations** are accounted for: **70 adapted** to generic contracts and **93 excluded** with the outside-D1 reason below. A property declaration counts once; generated cases do not inflate the inventory. Adapted means the named generic obligation is exercised, with changed or excluded M3 assertions stated explicitly; it does not mean a byte-for-byte test move.

The D1 test links resolve against this branch. The final-base workspace all-features suite passes 330 tests with zero failures and two inherited opt-in tests ignored; this includes all 61 receiver and 57 lease tests, with zero scoped ignores. Format, workspace clippy with warnings denied, query metadata freshness and wasm checks pass. This ledger does not claim hosted CI, process-kill, WebSocket, or D2 writer verification.

Copy rules run against actual Turso transactions. Durable-operation tests use the actual engine and file-backed Turso journal, copy rules and call-keyed consumer gate receipts. Reopen cases close and reopen files in one process. Holder release/drain is a receipted stand-in for D2's writer; it cannot establish the real writer interaction. The controlled capacity case injects SQLITE_FULL after a real write, not physical disk exhaustion. Pure SQL planning and fragment assembly tests remain pure.

Approved adaptations: copy-first initialization replaces pending-lease publication; immediate epoch reservations never rewind, including a second floor-repair refusal; terminal retirement replaces board archived envelopes; replicated membership excludes consumer-local tables; writers own FK integrity and copy enforcement is off on both runtimes (decision 34). M3 has no dedicated FK test declaration in this inventory. Board enrollment, archive reports, directory/Fleet projections, forwarding and health diagnostics remain consumer scope. Session and transport responsibilities belong to C and remain outside D1.

## Inventory

| M3 source | Test | D1 disposition |
| --- | --- | --- |
| `src/bound_sql.rs:99` | `typed_casts_preserve_placeholders_and_do_not_touch_question_marks_in_literals` | Adapted: [T1](#d1-test-index) |
| `src/bound_sql.rs:123` | `typed_read_plan_decodes_classes_and_rejects_bad_or_duplicate_aliases` | Adapted: [T2](#d1-test-index) |
| `tests/access_enrollment.rs:6` | `access_client_id_and_machine_credential_bind_to_the_same_enrollment` | Excluded: consumer enrollment, credentials, Access authentication and admission |
| `tests/access_rules.rs:28` | `supplied_clock_exact_expiry_and_not_before_boundaries_are_enforced` | Excluded: consumer enrollment, credentials, Access authentication and admission |
| `tests/access_rules.rs:61` | `observed_service_token_payload_parses_as_machine` | Excluded: consumer enrollment, credentials, Access authentication and admission |
| `tests/access_rules.rs:78` | `single_string_audience_for_another_app_is_refused` | Excluded: consumer enrollment, credentials, Access authentication and admission |
| `tests/access_rules.rs:92` | `missing_malformed_algorithm_issuer_audience_and_path_fail_closed` | Excluded: consumer enrollment, credentials, Access authentication and admission |
| `tests/access_rules.rs:149` | `caller_is_not_released_until_the_signature_port_returns_valid` | Excluded: consumer enrollment, credentials, Access authentication and admission |
| `tests/access_rules.rs:183` | `owner_is_an_identity_assertion_and_must_match_the_configured_identity` | Excluded: consumer enrollment, credentials, Access authentication and admission |
| `tests/access_rules.rs:236` | `jwks_selects_only_one_compatible_key_by_kid` | Excluded: consumer enrollment, credentials, Access authentication and admission |
| `tests/archive_report_storage.rs:43` | `first_report_retains_all_evidence_across_successor_resends_and_completion` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/archive_report_storage.rs:94` | `typed_report_refusals_keep_records_reports_feed_and_ledger_unchanged` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/archive_report_storage.rs:162` | `finalization_failure_rolls_back_report_board_workspace_and_feed_together` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/archive_report_storage.rs:211` | `corrupt_report_metadata_is_rejected_without_silently_counting_a_report` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/bookkeeping_migrations.rs:34` | `tolerant_reader_distinguishes_absent_matching_damaged_and_recorded_metadata` | Excluded: central legacy bookkeeping-version adoption and failure diagnostics; generic new schema uses owned SQLx migrations |
| `tests/bookkeeping_migrations.rs:82` | `tolerant_reader_rejects_a_partial_or_semantically_changed_legacy_shape` | Excluded: central legacy bookkeeping-version adoption and failure diagnostics; generic new schema uses owned SQLx migrations |
| `tests/bookkeeping_migrations.rs:109` | `recorded_metadata_costs_a_schema_probe_and_one_version_read_without_a_cache` | Excluded: central legacy bookkeeping-version adoption and failure diagnostics; generic new schema uses owned SQLx migrations |
| `tests/bookkeeping_migrations.rs:150` | `upgrading_recorded_v1_does_not_stamp_a_malformed_existing_outbox_as_current` | Excluded: central legacy bookkeeping-version adoption and failure diagnostics; generic new schema uses owned SQLx migrations |
| `tests/bookkeeping_migrations.rs:174` | `tolerant_reader_refuses_empty_or_invalid_recorded_versions` | Excluded: central legacy bookkeeping-version adoption and failure diagnostics; generic new schema uses owned SQLx migrations |
| `tests/bookkeeping_migrations.rs:199` | `late_migration_sql_failure_rolls_back_its_write_and_records_diagnostics_separately` | Excluded: central legacy bookkeeping-version adoption and failure diagnostics; generic new schema uses owned SQLx migrations |
| `tests/bookkeeping_migrations.rs:323` | `full_during_the_first_migration_preserves_its_type_and_can_record_after_rollback` | Excluded: central legacy bookkeeping-version adoption and failure diagnostics; generic new schema uses owned SQLx migrations |
| `tests/bookkeeping_migrations.rs:420` | `first_copy_event_versions_an_uninitialized_store` | Excluded: central legacy bookkeeping-version adoption and failure diagnostics; generic new schema uses owned SQLx migrations |
| `tests/bookkeeping_migrations.rs:451` | `populated_preversioned_copy_is_adopted_without_changing_project_rows_or_position` | Excluded: central legacy bookkeeping-version adoption and failure diagnostics; generic new schema uses owned SQLx migrations |
| `tests/claim_archive_repair.rs:196` | `reservation_committed_before_archive_survives_lost_answer_and_registry_restart` | Adapted: [T3](#d1-test-index). Generic gate reservation and same CallId survive journal reopen; board archive records stay consumer-owned. |
| `tests/claim_archive_repair.rs:235` | `fencing_is_not_abandoned_on_timeout_and_a_committed_fence_replays_after_restart` | Adapted: [T3](#d1-test-index). Both unsent and lost fence answers retain the attempt; no second epoch or settlement. |
| `tests/claim_archive_repair.rs:291` | `refused_cleanup_repairs_dropped_release_and_lost_ack_across_restarts_without_synthetic_report` | Adapted: [T4](#d1-test-index), [T5](#d1-test-index). Generic settlement repairs reserved refusals and retained grants; no board archive-report behavior is claimed. |
| `tests/claim_archive_repair/unavailable.rs:3` | `unavailable_claim_restore_probe_keeps_active_reservation_until_actual_refusal` | Adapted: [T3](#d1-test-index), [T6](#d1-test-index). Copy-status outage retries; a real NotInitialized answer terminates Unavailable and settles the reserved attempt. |
| `tests/claim_archive_repair/unavailable.rs:73` | `unavailable_claim_fence_keeps_active_fleet_reservation_and_replays_after_repair` | Adapted: [T3](#d1-test-index). Generic gate owns the reservation; Fleet projection is excluded. |
| `tests/claim_archive_repair/unavailable.rs:143` | `unavailable_claim_schema_probe_keeps_the_attempt_retryable_without_recording_a_rejection` | Adapted: [T3](#d1-test-index), [T7](#d1-test-index). The old separate board schema probe is replaced by copy status/fence; transient transport and typed schema refusal are exercised separately. |
| `tests/copy_cases/extra_cases.rs:24` | `old_epoch_cutoff_does_not_follow_later_replication_or_later_fences` | Adapted: [T8](#d1-test-index) |
| `tests/copy_cases/extra_cases.rs:87` | `not_initialized_archived_future_epoch_and_sender_digest_mismatch_are_distinct` | Adapted: [T9](#d1-test-index), [T10](#d1-test-index), [T11](#d1-test-index). Archived maps to the permanent Retired protocol answer. |
| `tests/copy_cases/extra_cases.rs:138` | `applied_part_receipts_retain_each_parts_digest_and_original_ack` | Adapted: [T12](#d1-test-index), [T13](#d1-test-index) |
| `tests/copy_cases/extra_cases.rs:169` | `count_and_whole_digest_disagreement_cannot_combine_fragments` | Adapted: [T14](#d1-test-index), [T15](#d1-test-index), [T12](#d1-test-index). Assembly rejects count/digest mismatches; the real host rolls back and discards poisoned staging in a separate transaction. |
| `tests/copy_cases/extra_cases.rs:194` | `snapshot_pages_are_read_only_position_labelled_and_restart_after_copy_moves` | Adapted: [T16](#d1-test-index) |
| `tests/copy_cases/extra_cases.rs:253` | `schema_and_rows_roll_back_together_on_a_late_constraint_error` | Adapted: [T17](#d1-test-index) |
| `tests/copy_cases/extra_cases.rs:297` | `engine_and_replication_bookkeeping_do_not_change_schema_identity` | Adapted: [T18](#d1-test-index) |
| `tests/copy_cases/extra_cases.rs:328` | `platform_tables_are_absent_from_snapshots_and_cannot_receive_replicated_rows` | Adapted: [T18](#d1-test-index), [T19](#d1-test-index). Persisted replicated membership excludes engine, platform and hook-local tables. |
| `tests/copy_operations.rs:83` | `health_is_unit_transport_and_reads_old_metadata_without_running_the_prelude` | Excluded: copy health/retirement and central object diagnostics |
| `tests/copy_operations.rs:146` | `health_reports_the_last_new_piece_and_staged_parts_without_advancing_on_replay` | Excluded: copy health/retirement and central object diagnostics |
| `tests/copy_operations.rs:307` | `legacy_health_uses_receipts_and_staged_times_without_treating_import_as_a_piece` | Excluded: copy health/retirement and central object diagnostics |
| `tests/copy_rules.rs:176` | `incoming_call_produces_exactly_one_return` | Excluded: central object-call return validation; D1 exports typed rule results |
| `tests/copy_rules.rs:188` | `call_conformance_rejects_missing_duplicate_wrong_id_and_wrong_kind_returns` | Excluded: central object-call return validation; D1 exports typed rule results |
| `tests/copy_rules.rs:222` | `ordered_apply_uses_declared_keys_and_preserves_commit_order` | Adapted: [T20](#d1-test-index), [T21](#d1-test-index). Real SQLx scopes preserve declared key order and sequential committed row images. |
| `tests/copy_rules.rs:257` | `repeat_returns_its_original_ack_after_later_pieces_and_changed_digest_is_refused` | Adapted: [T21](#d1-test-index), [T13](#d1-test-index) |
| `tests/copy_rules.rs:278` | `missing_receipt_is_never_blindly_acknowledged_and_gap_names_current_position` | Adapted: [T13](#d1-test-index), [T11](#d1-test-index) |
| `tests/copy_rules.rs:306` | `fence_survives_memory_loss_and_stale_epoch_uses_its_original_cutoff` | Adapted: [T8](#d1-test-index) |
| `tests/copy_rules.rs:344` | `fence_checks_preview_position_schema_and_forward_motion_in_the_transaction` | Adapted: [T22](#d1-test-index) |
| `tests/copy_rules.rs:406` | `two_part_transaction_is_invisible_until_complete_and_each_part_replays_after_commit` | Adapted: [T12](#d1-test-index) |
| `tests/copy_rules.rs:434` | `repeated_staged_part_matches_piece_digest_and_changed_content_conflicts` | Adapted: [T12](#d1-test-index) |
| `tests/copy_rules.rs:452` | `missing_middle_and_cleanup_before_final_never_apply_a_short_transaction` | Adapted: [T23](#d1-test-index), [T12](#d1-test-index). Assembly exercises a missing middle; real rollback/discard/resend exercises host cleanup without publishing a prefix. |
| `tests/copy_rules.rs:491` | `whole_transaction_digest_mismatch_drops_staging_without_applying` | Adapted: [T15](#d1-test-index), [T12](#d1-test-index) |
| `tests/copy_rules.rs:518` | `copy_full_rolls_back_all_rows_and_ack_before_returning_typed_refusal` | Adapted: [T24](#d1-test-index). Deterministic SQLITE_FULL injection at the SQLx Executor after a real first-row write; actual Turso rollback. Physical disk exhaustion is not claimed. |
| `tests/copy_rules.rs:543` | `corrupt_stored_state_fails_closed_instead_of_defaulting` | Adapted: [T25](#d1-test-index) |
| `tests/copy_unavailable.rs:6` | `admin_unavailability_uses_the_same_closed_subject_contract` | Excluded: central admin blocked-subject envelope; D1 returns typed rule/storage errors |
| `tests/fence_floor_repair.rs:59` | `floor_repair_refused_again_is_unproven_without_grant_notice_lease_or_floor_change` | Adapted: [T26](#d1-test-index). Approved semantic change: immediate reservation retains repaired floor 6, rather than M3 floor 1; no grant or lease change and no epoch reuse. |
| `tests/fleet_archive.rs:12` | `released_attempts_never_reactivate_or_release_a_different_attempt` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_archive.rs:46` | `releasing_the_last_reservation_finishes_archive_without_a_synthetic_report` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_archive.rs:99` | `archive_before_claim_returns_archived_without_fencing_or_granting` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_archive.rs:179` | `a_grant_at_the_unheld_timestamp_cannot_leave_archive_eligible_metadata` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_archive.rs:274` | `archive_reports_finalize_with_original_actors_without_a_request_ledger_entry` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_directory.rs:83` | `real_fleet_keeps_summary_by_position_and_lease_by_independent_registry_version` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_directory.rs:105` | `unheld_directory_update_changes_only_lease_and_never_creates_fleet_records` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_directory.rs:268` | `public_directory_keeps_partial_sides_visible_and_filters_only_known_metadata` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_directory.rs:370` | `public_directory_exposes_independent_freshness_without_late_or_equal_rewinds` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_directory.rs:427` | `public_directory_refuses_an_invalid_typed_query` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_feed.rs:172` | `arbitrary_import_and_workspace_archive_have_contiguous_positions_without_id_stock` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_feed.rs:210` | `old_import_times_do_not_expire_feed_before_thirty_days_of_central_retention` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_feed.rs:239` | `fleet_cursor_kind_and_future_position_are_refused_without_rewinding` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_feed.rs:258` | `retention_alarm_prunes_every_entry_without_resetting_the_persisted_head` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_feed.rs:307` | `inconsistent_feed_record_position_is_rejected_without_advancing_the_reader` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_lease_order.rs:162` | `fenced_claim_repair_keeps_exact_settlement_identity_across_new_mutation_and_lost_ack` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_lease_order.rs:281` | `delayed_unseen_incarnation_eight_is_fenced_after_another_project_advanced_the_witness` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_lease_order.rs:312` | `tuple_admission_keeps_the_whole_projection_and_as_of_does_not_order_it` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_lease_order.rs:365` | `fenced_claim_grant_applies_nothing_and_newer_lease_does_not_settle_another_attempt` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_lease_order.rs:439` | `settlement_storage_failure_rolls_back_the_lease_projection_in_the_same_transaction` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_lease_order.rs:482` | `invalid_incoming_digest_and_corrupt_stored_versions_fail_without_coercion` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_notice_outbox.rs:8` | `fleet_mutation_only_owes_a_hint_then_alarm_retries_and_correlates_real_delivery_ack` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_notice_outbox.rs:152` | `a_later_alarm_drains_more_readers_than_one_shell_id_batch_without_losing_debt` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_notice_outbox.rs:198` | `workspace_archive_owes_one_board_hint_to_each_known_holder` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_notice_outbox.rs:258` | `a_late_outbox_failure_rolls_back_records_feed_ledger_and_high_water_together` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/fleet_requests.rs:78` | `decided_replay_and_conflicting_request_preserve_every_fleet_row_and_feed_entry` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/forward_rules.rs:106` | `slot_is_reserved_before_route_and_payload_is_never_stored` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/forward_rules.rs:128` | `terminal_route_failure_and_deadline_free_slots_and_ignore_late_results` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/forward_rules.rs:174` | `delivery_and_answer_authority_preserve_original_deadline_and_drop_late_delivery` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/forward_rules.rs:227` | `authorized_answer_and_no_connection_each_release_the_reserved_slot` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/forward_rules.rs:282` | `expired_machine_cannot_answer_a_forward_and_its_session_closes` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/forward_rules.rs:305` | `holder_storage_excludes_content_and_failed_delivery_frees_the_sender_slot` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/forward_rules.rs:396` | `delivery_alarm_owns_only_delivered_rows_and_preserves_the_session_deadline` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/import_claim.rs:165` | `staged_import_is_invisible_and_reimport_never_changes_the_summary` | Adapted: [T27](#d1-test-index), [T28](#d1-test-index), [T29](#d1-test-index), [T30](#d1-test-index). Generic seed visibility, immutable import identity and snapshot-derived hook replace board summary assertions; directory publication is consumer-owned. |
| `tests/import_claim.rs:195` | `claim_fences_before_grant_and_one_attempt_keeps_one_epoch_and_expiry` | Adapted: [T31](#d1-test-index), [T3](#d1-test-index) |
| `tests/import_claim.rs:268` | `copy_fence_restore_guard_refuses_claim_without_grant_or_floor_repair` | Adapted: [T6](#d1-test-index). Unseeded copy refuses Unavailable and settles; the newly reserved floor remains 2 under the approved monotonic-reservation model. |
| `tests/import_claim.rs:293` | `late_directory_ack_cannot_remove_a_newer_summary_outbox_row` | Excluded: board directory-summary outbox acknowledgement ordering; D1 defines neither directory rows nor that outbox. |
| `tests/import_claim.rs:382` | `stored_import_metadata_for_another_project_is_rejected_explicitly` | Adapted: [T57](#d1-test-index); receiver seed metadata identity |
| `tests/import_claim.rs:411` | `rerunning_import_repairs_the_copy_to_registry_publication_gap` | Adapted: [T33](#d1-test-index) |
| `tests/import_claim.rs:444` | `claimed_copy_preserves_holder_change_floor_repair_semantics` | Adapted: [T34](#d1-test-index) |
| `tests/lease_order.rs:34` | `fleet_incarnation_fence_advances_above_both_durable_and_offered_floors` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/lease_order.rs:54` | `lease_versions_order_epoch_then_incarnation_then_change_and_reject_zero_components` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/lease_order.rs:86` | `a_restarted_registry_decides_renewal_but_gates_publication_until_fleet_allocation` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/numeric_transport.rs:3` | `integer_text_is_decoded_without_a_double_and_real_kind_is_retained` | Adapted: [T35](#d1-test-index), [T36](#d1-test-index). SQLx transport retains integer text casts, real classes and signed infinities. |
| `tests/numeric_transport.rs:18` | `invalid_integer_text_is_a_stored_integrity_error` | Adapted: [T2](#d1-test-index). Malformed and overflowing integer-text values fail closed in the decoder. |
| `tests/registry_lease_publication.rs:10` | `renewal_counter_attempt_and_publication_debt_commit_or_roll_back_together` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/registry_lease_publication.rs:57` | `lost_allocation_answer_and_crash_before_installation_keep_the_run_gated` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/registry_lease_publication.rs:100` | `registry_restored_to_three_gets_ten_from_fleets_retained_nine` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/registry_lease_publication.rs:159` | `reordering_changes_and_replaying_attempts_never_allocate_or_republish` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/registry_lease_publication.rs:216` | `obsolete_fenced_conflict_and_success_answers_cannot_clear_or_restart_newer_work` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/registry_lease_publication.rs:273` | `fenced_and_conflicting_current_offers_rebuild_from_current_rows` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/registry_lease_publication.rs:323` | `unavailable_fleet_leaves_publication_gated_but_renewal_and_replay_authoritative` | Excluded: board archive/Fleet/admission/directory behavior |
| `tests/registry_rules.rs:6` | `renewal_replays_expiry_without_second_renewal_then_fresh_attempt_renews` | Adapted: [T37](#d1-test-index) |
| `tests/registry_rules.rs:36` | `expiry_is_computed_on_supplied_clock_and_same_holder_recovers_same_epoch` | Adapted: [T37](#d1-test-index) |
| `tests/registry_rules.rs:53` | `lost_renewal_replayed_after_takeover_reports_current_holder_then_fresh_old_attempt_stale` | Adapted: [T38](#d1-test-index) |
| `tests/registry_rules.rs:79` | `changed_renewal_identity_is_refused_and_attempts_survive_time_without_cleanup` | Adapted: [T39](#d1-test-index), [T37](#d1-test-index) |
| `tests/registry_rules.rs:92` | `first_epoch_requires_fleet_acceptance_and_duplicate_creation_does_not_renew` | Adapted: [T40](#d1-test-index), [T41](#d1-test-index). Generic consumer admission replaces Fleet; the copy exists before the epoch-one grant. |
| `tests/registry_rules.rs:116` | `admin_methods_reject_machine_and_agent_with_no_effect` | Excluded: consumer admin authentication; generic registry exposes rules, no admin route |
| `tests/registry_rules.rs:142` | `renewal_replay_never_extends_expiry_for_any_elapsed_time` | Adapted: [T37](#d1-test-index). M3 property declaration maps to a bounded set of supplied-clock boundaries, including a far-later replay; no claim of exhaustive time sampling. |
| `tests/registry_rules.rs:149` | `new_epoch_strictly_exceeds_every_proven_registry_floor` | Adapted: [T42](#d1-test-index), [T43](#d1-test-index), [T44](#d1-test-index). M3 property declaration maps to concrete low, restored, inconsistent and u64-exhausted floors. |
| `tests/registry_unavailable.rs:59` | `first_creation_retains_its_exact_pending_lease_and_retries_the_same_epoch` | Adapted: [T45](#d1-test-index), [T41](#d1-test-index). Approved copy-first model has no pending lease: lost creation answer leaves zero grants; matching creation can finish epoch one, a different writer is refused. |
| `tests/registry_unavailable.rs:100` | `preview_unavailability_has_project_identity_and_does_not_make_a_transfer` | Adapted: [T46](#d1-test-index). Typed target carries database identity; board dispatch envelope is excluded. |
| `tests/registry_unavailable.rs:135` | `unavailable_fence_keeps_transfer_epoch_and_lease_then_same_request_grants_once` | Adapted: [T3](#d1-test-index). Same generic fence call and epoch survive both unsent and lost answers. |
| `tests/registry_unavailable.rs:176` | `unavailable_holder_release_keeps_fencing_and_the_same_release_finishes` | Adapted: [T47](#d1-test-index), [T48](#d1-test-index). Lost release answers remain retryable under one CallId; only never-sent calls have the configured bound. |
| `tests/registry_unavailable.rs:223` | `unavailable_release_schema_probe_keeps_its_chosen_state_until_real_schema_refusal` | Adapted: [T47](#d1-test-index), [T49](#d1-test-index). The separate board schema probe is removed; generic release transport retries and typed schema refusals are exercised separately. |
| `tests/registry_unavailable.rs:287` | `later_clock_grant_replay_keeps_expiry_history_and_fence_receipt_without_new_effects` | Adapted: [T3](#d1-test-index), [T8](#d1-test-index). Full grant answer and history count remain stable after a later clock and replay; copy fence receipt remains immutable. |
| `tests/rejected_tail.rs:119` | `historical_holder_is_verified_before_its_tail_reaches_the_real_fleet` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/rejected_tail.rs:136` | `never_writer_is_refused_without_a_fleet_store_or_tail_notice` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/rejected_tail.rs:194` | `first_tail_wins_with_exact_conflict_and_no_duplicate_hint_or_request_ledger` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/rejected_tail.rs:261` | `full_tail_read_is_project_scoped_and_orders_numeric_epochs` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/rejected_tail.rs:294` | `holder_change_after_verification_cannot_strand_the_durable_report` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/rejected_tail.rs:356` | `replaced_or_expired_session_never_stores_a_late_verification_result` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/rejected_tail.rs:407` | `a_late_hint_failure_rolls_back_tail_and_corrupt_stored_evidence_is_refused` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/retired_gate.rs:63` | `retired_marker_precedes_even_a_newer_ledger_and_operations_do_no_work` | Adapted: [T50](#d1-test-index), [T29](#d1-test-index). Copy marker is read before other bookkeeping; registry host checks lifecycle before returning cached task answers. |
| `tests/retired_gate.rs:104` | `retired_health_is_readable_from_the_minimal_receipt_without_other_bookkeeping` | Adapted: [T50](#d1-test-index). Generic copy status and terminal data answers work after all other replication tables are dropped. The board health envelope is excluded. |
| `tests/retired_gate.rs:141` | `health_reports_a_damaged_retire_receipt_instead_of_failing_the_whole_diagnostic` | Excluded: consumer health diagnostic envelope with a damaged-retirement subreport; D1 has no health API. Generic corrupt-state reads fail closed. |
| `tests/retired_gate.rs:173` | `retired_data_calls_keep_their_typed_piece_read_and_import_results` | Adapted: [T50](#d1-test-index), [T29](#d1-test-index) |
| `tests/session_notices.rs:127` | `delivery_assigns_its_own_positive_positions_and_session_reads_locally` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/session_notices.rs:163` | `disconnected_notice_is_durable_and_prune_all_never_resets_its_counter` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/session_notices.rs:229` | `fleet_hint_uses_delivery_sequence_instead_of_fleet_cursor_and_old_rpc_is_closed` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/session_notices.rs:249` | `a_late_counter_failure_rolls_back_the_notice_and_next_position` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/session_rules.rs:69` | `hello_welcome_reconstructs_ack_and_current_lease_from_separate_objects` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/session_rules.rs:76` | `replacement_targets_old_socket_and_drops_its_inflight_answer_then_resend_replays` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/session_rules.rs:107` | `hello_rejects_unknown_expired_revoked_and_non_machine_credentials` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/session_rules.rs:132` | `expiry_mid_session_closes_without_dispatching_request` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/session_rules.rs:156` | `decode_failures_preserve_connection_and_expiry_guards_without_request_dispatch` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/session_rules.rs:256` | `revoke_closes_current_session_and_authentication_stays_revoked` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/session_rules.rs:281` | `direct_copy_unavailability_answers_the_original_frame_without_closing_the_session` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/session_rules.rs:369` | `fleet_creation_reports_unavailable_initial_lease_instead_of_its_prior_acceptance` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/session_rules.rs:443` | `every_request_kind_dispatches_typed_result_and_answers_exact_frame_id_once` | Excluded: session/transport/forwarding, central envelopes, or board tail reports |
| `tests/transfer_rules.rs:14` | `piece_between_confirmation_and_fence_refuses_without_grant` | Adapted: [T51](#d1-test-index) |
| `tests/transfer_rules.rs:30` | `schema_support_checked_before_fence_and_unknown_target_refused` | Adapted: [T22](#d1-test-index), [T7](#d1-test-index). Generic configured schema maximum is checked by the real copy fence. Unknown board enrollment targets are consumer admission, excluded from D1. |
| `tests/transfer_rules.rs:66` | `interrupted_fence_repeated_request_completes_same_epoch_and_conflicting_request_refused` | Adapted: [T3](#d1-test-index), [T8](#d1-test-index). Journal replay preserves grant identity; direct fence receipt rejects changed content. |
| `tests/transfer_rules.rs:94` | `move_pending_one_transfer_only_and_renewal_recovers_missed_release_notice` | Adapted: [T52](#d1-test-index) |
| `tests/transfer_rules.rs:133` | `migration_pending_or_failed_refusal_is_durable_and_changes_no_lease` | Adapted: [T49](#d1-test-index) |
| `tests/transfer_rules.rs:170` | `move_refuses_disconnected_holder_or_thirty_minute_boundary` | Adapted: [T48](#d1-test-index). Generic configurable never-sent bound replaces the consumer session-presence and fixed thirty-minute policy. |
| `tests/transfer_rules.rs:199` | `restored_registry_never_issues_epoch_at_or_below_copy_fence` | Adapted: [T42](#d1-test-index) |
| `tests/transfer_rules.rs:221` | `another_installation_cannot_write_known_current_epoch` | Adapted: [T11](#d1-test-index) |
| `tests/transfer_rules.rs:256` | `release_gap_can_be_retried_after_the_final_piece_reaches_copy` | Adapted: [T53](#d1-test-index) |
| `tests/transfer_rules.rs:283` | `schema_refusal_at_release_is_typed_replayed_and_never_grants` | Adapted: [T49](#d1-test-index) |
| `tests/transfer_rules.rs:309` | `historical_holder_gets_immutable_stale_cutoff_other_sender_gets_not_writer` | Adapted: [T8](#d1-test-index) |
| `tests/transfer_rules.rs:361` | `no_epoch_records_refuses_preview_without_grant_or_floor_write` | Adapted: [T54](#d1-test-index) |
| `tests/transfer_rules.rs:391` | `fence_not_advanced_retries_once_from_authoritative_copy_floor` | Adapted: [T55](#d1-test-index) |
| `tests/transfer_rules.rs:417` | `unknown_schema_at_release_is_recorded_before_reply_and_replays_after_interruption` | Adapted: [T49](#d1-test-index). Unknown support is a durable typed release refusal with supported=None. Transport interruption is separately covered by the lost-release test; no board schema lookup is claimed. |
| `tests/transfer_rules.rs:457` | `schema_piece_cannot_forge_fence_receipt_before_move_grant` | Adapted: [T19](#d1-test-index) |
| `tests/transfer_rules.rs:508` | `inconsistent_fence_receipt_cannot_complete_pending_transfer` | Adapted: [T8](#d1-test-index). Corrupt direct receipt fails closed; generic call dispatch leaves storage failures retryable and never invents a successful fence. |
| `tests/transfer_rules.rs:554` | `whole_piece_schema_refusal_applies_no_earlier_rows_or_schema` | Adapted: [T19](#d1-test-index) |
| `tests/transfer_rules.rs:603` | `mixed_case_reserved_row_table_is_rejected_without_mutation` | Adapted: [T56](#d1-test-index) |

## D1 test index

| ID | Current test |
| --- | --- |
| T1 | [`typed_casts_preserve_placeholders_and_do_not_touch_question_marks_in_literals`](../../crates/fleet-sync-receiver/src/bound_sql.rs#L104) |
| T2 | [`typed_read_plan_decodes_classes_and_rejects_bad_or_duplicate_aliases`](../../crates/fleet-sync-receiver/src/bound_sql.rs#L128) |
| T3 | [`claim_replays_reservation_and_fence_without_a_second_epoch_or_settlement`](../../crates/fleet-sync-lease/src/turso_claim_replay_tests.rs#L10) |
| T4 | [`version_failure_before_grant_records_failed_and_settles_reserved_claim_refused`](../../crates/fleet-sync-lease/src/turso_claim_recovery_tests.rs#L37) |
| T5 | [`version_failure_after_committed_grant_preserves_granted_and_settles_it_once`](../../crates/fleet-sync-lease/src/turso_claim_recovery_tests.rs#L102) |
| T6 | [`an_unseeded_copy_refuses_claim_and_settles_its_reserved_attempt`](../../crates/fleet-sync-lease/src/turso_claim_replay_tests.rs#L120) |
| T7 | [`schema_refusal_after_reservation_is_settled_and_a_gate_refusal_has_no_settlement`](../../crates/fleet-sync-lease/src/turso_claim_recovery_tests.rs#L202) |
| T8 | [`reopened_copy_keeps_old_fence_receipts_and_cutoffs_after_later_fences`](../../crates/fleet-sync-receiver/src/turso_fence_integrity_tests.rs#L67) |
| T9 | [`absent_copy_and_future_epoch_are_distinct_refusals_with_no_mutation`](../../crates/fleet-sync-receiver/src/turso_integrity_tests.rs#L291) |
| T10 | [`fencing_preserves_old_holder_cutoff_and_retirement_dominates_replays`](../../crates/fleet-sync-receiver/src/turso_rule_tests.rs#L324) |
| T11 | [`wrong_sender_digest_conflict_gap_and_unsupported_schema_leave_no_writes`](../../crates/fleet-sync-receiver/src/turso_rule_tests.rs#L101) |
| T12 | [`split_transaction_stays_invisible_until_final_and_failed_final_discards_separately`](../../crates/fleet-sync-receiver/src/turso_rule_tests.rs#L172) |
| T13 | [`changed_receipt_content_conflicts_and_missing_receipt_never_acknowledges_a_gap`](../../crates/fleet-sync-receiver/src/turso_integrity_tests.rs#L9) |
| T14 | [`mismatched_count_and_transaction_identity_cannot_combine_fragments`](../../crates/fleet-sync-receiver/src/part_assembly.rs#L178) |
| T15 | [`changed_transaction_content_is_rejected_even_with_valid_piece_digests`](../../crates/fleet-sync-receiver/src/part_assembly.rs#L160) |
| T16 | [`snapshot_pages_use_primary_key_cursor_and_stop_when_position_moves`](../../crates/fleet-sync-receiver/src/turso_rule_tests.rs#L446) |
| T17 | [`late_row_constraint_rolls_back_schema_membership_rows_and_position_together`](../../crates/fleet-sync-receiver/src/turso_integrity_tests.rs#L80) |
| T18 | [`hook_table_exists_but_never_enters_snapshot_pages_or_fingerprint`](../../crates/fleet-sync-receiver/src/turso_schema_isolation_tests.rs#L111) |
| T19 | [`protected_schema_is_rejected_before_earlier_rows_and_local_tables_cannot_receive_rows`](../../crates/fleet-sync-receiver/src/turso_integrity_tests.rs#L143) |
| T20 | [`composite_upsert_and_delete_use_declared_primary_key_order_on_turso`](../../crates/fleet-sync-receiver/src/turso_transport_tests.rs#L170) |
| T21 | [`repeated_piece_returns_original_position_without_repeating_hook_writes`](../../crates/fleet-sync-receiver/src/turso_rule_tests.rs#L9) |
| T22 | [`fence_checks_forward_motion_exact_preview_and_schema_before_mutation`](../../crates/fleet-sync-receiver/src/turso_fence_integrity_tests.rs#L23) |
| T23 | [`missing_or_duplicate_fragment_never_builds_a_short_transaction`](../../crates/fleet-sync-receiver/src/part_assembly.rs#L151) |
| T24 | [`copy_full_after_a_real_row_write_returns_the_typed_refusal_and_rolls_back_everything`](../../crates/fleet-sync-receiver/src/turso_capacity_tests.rs#L178) |
| T25 | [`corrupt_copy_records_fail_closed_for_reads_and_apply`](../../crates/fleet-sync-receiver/src/turso_integrity_tests.rs#L219) |
| T26 | [`a_second_floor_refusal_keeps_its_reservation_without_granting_or_changing_the_lease`](../../crates/fleet-sync-lease/src/turso_floor_repair_tests.rs#L157) |
| T27 | [`missing_pages_changed_page_and_expected_position_are_refused_without_publishing`](../../crates/fleet-sync-receiver/src/turso_seed_tests.rs#L395) |
| T28 | [`digest_refusal_rolls_back_created_schema_and_preserves_staging_for_correct_resend`](../../crates/fleet-sync-receiver/src/turso_seed_tests.rs#L325) |
| T29 | [`import_repeats_and_conflicts_then_retirement_precedes_cached_engine_answers`](../../crates/fleet-sync-lease/src/turso_lifecycle_tests.rs#L188) |
| T30 | [`seed_hook_and_follow_up_run_once_while_retained_piece_work_keeps_its_own_identity`](../../crates/fleet-sync-receiver/src/turso_seed_hook_tests.rs#L161) |
| T31 | [`claim_grants_the_next_epoch_and_settles_once_after_seed_publication`](../../crates/fleet-sync-lease/src/turso_change_tests.rs#L27) |
| T32 | [`stored_database_identity_mismatch_is_not_treated_as_an_import_replay`](../../crates/fleet-sync-lease/src/turso_registry_integrity_tests.rs#L82) |
| T33 | [`import_replays_the_copy_before_publishing_registry_state_after_lost_answer`](../../crates/fleet-sync-lease/src/turso_registry_integrity_tests.rs#L11) |
| T34 | [`claim_repairs_above_a_restored_imported_copys_fence_before_granting`](../../crates/fleet-sync-lease/src/turso_claim_replay_tests.rs#L185) |
| T35 | [`integer_real_text_blob_and_null_keep_exact_values_and_storage_classes`](../../crates/fleet-sync-receiver/src/turso_transport_tests.rs#L41) |
| T36 | [`positive_and_negative_infinity_remain_real_through_row_apply_and_exact_readback`](../../crates/fleet-sync-receiver/src/turso_transport_tests.rs#L96) |
| T37 | [`renewal_replay_never_extends_expiry_and_an_expired_current_holder_can_renew_the_same_epoch`](../../crates/fleet-sync-lease/src/turso_renew_tests.rs#L92) |
| T38 | [`replay_preserves_recorded_answer_but_returns_current_lease_after_later_renewal_and_takeover`](../../crates/fleet-sync-lease/src/turso_renew_tests.rs#L139) |
| T39 | [`changed_renewal_identity_is_an_error_and_cannot_extend_the_lease`](../../crates/fleet-sync-lease/src/turso_renew_tests.rs#L287) |
| T40 | [`refused_registration_changes_no_copy_or_authority_and_a_new_attempt_can_succeed`](../../crates/fleet-sync-lease/src/turso_lifecycle_tests.rs#L142) |
| T41 | [`register_creates_copy_before_grant_and_replay_never_issues_another_epoch`](../../crates/fleet-sync-lease/src/turso_lifecycle_tests.rs#L35) |
| T42 | [`restored_registry_reserves_above_the_fresh_previews_copy_fence`](../../crates/fleet-sync-lease/src/turso_floor_repair_tests.rs#L39) |
| T43 | [`floor_behind_the_current_lease_cannot_reuse_a_previously_granted_epoch`](../../crates/fleet-sync-lease/src/turso_epoch_proof_tests.rs#L98) |
| T44 | [`missing_or_exhausted_floor_refuses_without_changing_authority_or_calling_peers`](../../crates/fleet-sync-lease/src/turso_epoch_proof_tests.rs#L44) |
| T45 | [`failed_registration_leaves_no_grant_and_original_creation_prevents_a_different_writer`](../../crates/fleet-sync-lease/src/turso_lifecycle_tests.rs#L71) |
| T46 | [`unavailable_preview_saves_no_confirmation_or_epoch_until_the_copy_answers`](../../crates/fleet-sync-lease/src/turso_epoch_proof_tests.rs#L141) |
| T47 | [`lost_release_answer_replays_one_call_even_past_the_unsent_bound_and_keeps_rows`](../../crates/fleet-sync-lease/src/turso_change_tests.rs#L99) |
| T48 | [`never_sent_move_ends_unreachable_without_fencing_or_changing_holder`](../../crates/fleet-sync-lease/src/turso_change_tests.rs#L160) |
| T49 | [`typed_release_refusals_replay_without_fencing_or_changing_the_lease`](../../crates/fleet-sync-lease/src/turso_move_refusal_tests.rs#L13) |
| T50 | [`retired_marker_answers_without_any_other_replication_bookkeeping`](../../crates/fleet-sync-receiver/src/turso_seed_tests.rs#L446) |
| T51 | [`takeover_refuses_a_moved_preview_then_new_preview_uses_a_later_reserved_epoch`](../../crates/fleet-sync-lease/src/turso_change_tests.rs#L231) |
| T52 | [`a_pending_move_blocks_another_move_and_renewal_returns_fresh_authority_after_grant`](../../crates/fleet-sync-lease/src/turso_registry_integrity_tests.rs#L147) |
| T53 | [`released_final_position_waits_for_the_real_copy_then_grants_once`](../../crates/fleet-sync-lease/src/turso_move_refusal_tests.rs#L110) |
| T54 | [`absent_copy_and_floor_cannot_create_a_preview_or_manufacture_epoch_evidence`](../../crates/fleet-sync-lease/src/turso_epoch_proof_tests.rs#L205) |
| T55 | [`fence_not_advanced_repairs_once_from_the_real_copys_proven_fence`](../../crates/fleet-sync-lease/src/turso_floor_repair_tests.rs#L93) |
| T56 | [`a_mixed_case_reserved_row_target_cannot_change_copy_bookkeeping`](../../crates/fleet-sync-receiver/src/turso_integrity_tests.rs#L248) |
| T57 | [`seed_complete_rejects_another_stored_database_before_publication_hook_or_admission`](../../crates/fleet-sync-receiver/src/turso_seed_hook_tests.rs#L427) |

## Additional final-target coverage

Decision 42 adds real-journal proof of a permanent follow-up failure ending with its code, Retry then Done under one CallId, and a saved Failed answer consumed after file reopen without another hook call. The interruption fixture stops after answer commit, before final operation completion; it is not a process-kill test. The registry inventory read is checked, read-only and validates row identity; it excludes Retired and unknown IDs, retaining Registered, Unheld and Retiring records.

The paired observation also identified gaps in reserved-Claim refusal coverage and explicit successful Takeover replay. `turso_claim_refusal_tests` now exercises Retired at status, NotInitialized/Retired at fence, repaired PreviewStale, and a second FenceNotAdvanced becoming EpochUnprovable. Every case reads the actual gate settlement and proves no registry grant; existing tests cover NotInitialized status, SchemaTooNew, and engine failures before/after grant. Gate refusal precedes reservation and owes no settlement. The successful Takeover case in `turso_floor_repair_tests` now resubmits the same request key after advancing the clock and verifies the identical answer with unchanged epoch, history and call count. No product regression was observed for these added coverage cases.

Decision 43 adds three real-Turso authority cases: writer-created copies accept only their original Writer, imported copies accept only Admin, and fencing to another current holder never rewrites creation authority. Unauthorized begin/new-generation/replay, existing/new page and completion calls commit their error transactions and leave the complete seed/copy/creation/schema readback unchanged, before and after publication. Retirement still precedes erased creation bookkeeping. These cases failed against the compilable authority-argument scaffold, then passed after authorization was wired.

Additional real-store regressions cover successive idle Moves and Move immediately after Claim or Takeover, preserving predecessor positions/rows and exact request replay, with future releases still rejected. Five valid-shaped fence-receipt corruption cases exercise both replay and historical stale-piece consumption with unchanged persisted readbacks. These are new generic-contract regressions; the original 163 declaration dispositions remain unchanged.

T57 retains the M3 copy-side metadata identity obligation at the receiver boundary. Three real-Turso regressions corrupt only the pending seed's database, then reject begin replay/new generation, page replay/new staging and completion with a typed corruption error. Committed error transactions preserve copy, seed, pages, attempts, creation, schema, hook rows and journal identities; no snapshot callback, new follow-up admission or transition to following occurs. T32 remains separate registry identity coverage and does not substitute for the receiver check. The inventory stays 70 adapted / 93 excluded; this corrects the adaptation target for the existing M3 declaration.

The workflow extension adds admission identity, phase/history and admin inspection, cancellation before fencing, move withdrawal and claim settlement debt. It retains the existing holder-change, renewal, floor and supersession assertions against the same real stores and engine. Original M3 declarations remain 163; additional workflow regressions do not change that inventory.
