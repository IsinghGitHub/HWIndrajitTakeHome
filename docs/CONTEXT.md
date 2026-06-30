# Provider Network Pipeline

Network adequacy / competitive comparison reporting for Medicare Advantage networks: for a payer's own network and one or more competitor networks, work out which providers are shared and which are unique, broken down by county and provider type, with cost/quality/utilization scores layered on top.

## Language

**Base Network**:
The payer's own provider network being evaluated in a comparison run. Identified by `baseParentOrgId` plus plan type.
_Avoid_: Primary network, our network

**Compare Network**:
A competitor network the base network is benchmarked against. A single run can have several compare networks, but they're evaluated one pairing (base + one compare) at a time, never all at once.
_Avoid_: Target network

**Common Provider**:
A provider (NPI) present in both networks of a base/compare pairing, for the same county and provider type.
_Avoid_: Shared, overlapping, duplicate

**Unique Provider**:
A provider present in only one side of a base/compare pairing.

**Provider Type**:
The category a provider falls into — PCP, Physician Specialist, Other Provider, or Hospital. Stored as `New Flag` in the source files; `Provider Type` is the name used in output.
_Avoid_: New Flag (legacy source-column name — don't carry it into new code or docs)

**Specialty Flag**:
Whether a provider's listed specialty is their Primary or Secondary one. Not applicable to organizations/hospitals.

**Network ID** (composite):
The key `<parentOrgId>_<planTypeId>` identifying a network within a comparison run — not the same thing as the raw CMS network IDs (`networkIds`) used to locate source files on disk. One Network ID can span several raw network IDs.
_Avoid_: Using "network_id" for both meanings interchangeably
