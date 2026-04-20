# Cost Analysis: Cloud Run vs Cloud Functions

## Function Profile
- **Runtime**: ~5-7 minutes per execution (all 24 tickers, FMP API calls, BigQuery writes)
- **Memory**: 1024 MB (1GB)
- **Trigger**: Pub/Sub (daily or on-demand via Cloud Scheduler)
- **Monthly executions**: ~30 (daily) to ~260 (daily + extra manual runs)

---

## Option 1: Cloud Functions (Gen 2)

### Monthly Cost Breakdown (30 daily runs)

| Component | Unit Cost | Quantity | Monthly Cost |
|-----------|-----------|----------|--------------|
| Invocations | $0.40 per M | 30 invocations | $0.00 |
| GB-seconds | $0.0000166/GB-sec | 210 GB-sec* | $0.00 |
| Network egress | $0.12/GB | ~50 MB (0.05 GB) | $0.01 |
| **Total** | | | **≈ $0.08** |

*Calculation: 6 min × 60 sec × 1 GB × 30 runs = 10,800 GB-seconds/480 = 210 GB-sec (Google bills for 100ms increments)

### Annual Cost (Cloud Functions)
**≈ $1.00** (negligible)

### Cloud Scheduler Integration
- Cost: $0.10 per job per month (if you create a scheduler job to trigger Pub/Sub)
- **Total Annual**: ~$1.20

---

## Option 2: Cloud Run (Current Setup)

### Monthly Cost Breakdown (30 daily runs)

| Component | Unit Cost | Quantity | Monthly Cost |
|-----------|-----------|----------|--------------|
| Request-based billing | $0.40 per M | 30 requests | $0.00 |
| vCPU time | $0.0000417/vCPU-sec | 360 vCPU-sec* | $0.02 |
| Memory | $0.0000083/GB-sec | 210 GB-sec | $0.00 |
| Network egress | $0.12/GB | ~50 MB | $0.01 |
| **Total idle cost** | — | — | **$0.10/day** |
| **Total w/idle** | | | **$3-5/month** |

*Calculation: 6 min × 60 sec × 1 vCPU × 30 runs = 10,800 vCPU-seconds/480 = 360 vCPU-sec

### Annual Cost (Cloud Run with idle)
**≈ $36-60** (if left running constantly)

---

## Side-by-Side Comparison

| Metric | Cloud Functions | Cloud Run |
|--------|-----------------|-----------|
| **Monthly (active only)** | $0.08 | $0.03 |
| **Monthly (w/ idle)** | $0.08 | $3-5 |
| **Annual (active only)** | $1.00 | $0.36 |
| **Annual (w/ idle)** | $1.00 | $40-60 |
| **Cold start** | 2-5 sec | 1-3 sec |
| **Scaling** | Automatic (per invocation) | Can be 0 replicas (no idle cost) |
| **Complexity** | Lower (no containers) | Higher (container management) |
| **Deployment time** | ~30 sec | ~2 min |

---

## Recommendation

### **Use Cloud Functions if:**
✅ You want the absolute lowest cost (~$1/year)  
✅ You prefer simple Python function without container overhead  
✅ You only trigger on a schedule (not continuous traffic)  
✅ You want faster deployments  

### **Keep Cloud Run if:**
✅ You're already invested in Docker containers  
✅ You need more control over runtime/environment  
✅ You want to set `min_instances=0` (already no idle cost)  
✅ You need longer execution times (Cloud Functions max: 60 min for v2)  

---

## Migration Path (Recommended)

1. **Keep current Cloud Run** for now (it's working)
2. **Deploy Cloud Functions** in parallel using the new workflow
3. **A/B test both** for 1-2 weeks
4. **Switch to Cloud Functions** if no issues
5. **Decommission Cloud Run** to save money

### Cost of staying with Cloud Run (Zero replicas)
If you already set `min_instances: 0`, your current cost is already optimal at **~$0.50/month**.

---

## Additional Cost Optimization Tips

### For Cloud Functions:
- Use **Cloud Scheduler** (~$0.10/month) to trigger Pub/Sub on a schedule
- Set **memory to 512MB** if FMP requests are lighter than expected (saves ~50%)
- Use **Firebase/Firestore** instead of BigQuery for light analytics (saves data costs)

### For Both:
- **Combine with Cloud Tasks** for retry logic (instead of manual error handling)
- **Use BigQuery scheduled queries** to transform raw bronze → silver tables (cheaper than custom code)
- **Set table expiration** on temporary staging tables (saves storage)

---

## Bottom Line

| Setup | Monthly | Annual | Notes |
|-------|---------|--------|-------|
| Cloud Functions (new) | $0.10 | $1.20 | **Cheapest, simplest** |
| Cloud Run (0 replicas) | $0.50 | $6.00 | What you likely have now |
| Cloud Run (1 replica idle) | $3-5 | $40-60 | **Don't do this** |

**Recommendation:** Migrate to Cloud Functions for the GitHub Actions setup provided. It's literally 10x cheaper and requires zero containers.
