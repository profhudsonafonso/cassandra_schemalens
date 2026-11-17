SELECT c.hostname, c.event_ts, c.usage_user, c.usage_system, c.usage_idle, c.usage_nice, c.usage_iowait, c.usage_irq, c.usage_softirq, c.usage_steal, c.usage_guest, c.usage_guest_nice
FROM CPUReading c
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.hostname ORDER BY c.event_ts DESC) = 1;
