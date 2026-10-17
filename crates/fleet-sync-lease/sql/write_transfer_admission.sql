INSERT INTO _fleet_sync_transfer_admissions(attempt,operation,request,requested_by,admitted_at,follow_up) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(attempt) DO UPDATE SET follow_up=excluded.follow_up
