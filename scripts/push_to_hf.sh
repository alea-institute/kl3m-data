#!/usr/bin/env bash

# source creds
source .env

#┏━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━┓
#┃ Dataset ID ┃ Documents ┃ Representations ┃ Parquet ┃
#┡━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━┩
#│ cap        │ ✓         │ ✓               │ ✓       │
#│ dockets    │ ✓         │ ✓               │ ✓       │
#│ dotgov     │ ✓         │ ✓               │ ✓       │     # sublist below
#│ ecfr       │ ✓         │ ✓               │ ✓       │
#│ edgar      │ ✓         │ ✓               │ ✓       │
#│ eu_oj      │ ✓         │ ✓               │ ✓       │
#│ fdlp       │ ✓         │ ✓               │ ✓       │
#│ fr         │ ✓         │ ✓               │ ✓       │
#│ govinfo    │ ✓         │ ✓               │ ✓       │     # sublist below
#│ recap      │ ✓         │ ✓               │ ✓       │
#│ recap_docs │ ✓         │ ✓               │ ✓       │
#│ reg_docs   │ ✓         │ ✓               │ ✓       │
#│ ukleg      │ ✓         │ ✓               │ ✓       │
#│ usc        │ ✓         │ ✓               │ ✓       │
#│ uspto      │ ✓         │ ✓               │ ✓       │
#└────────────┴───────────┴─────────────────┴─────────┘

L0_DATASETS="cap dockets ecfr edgar eu_oj fdlp fr recap recap_docs reg_docs ukleg usc uspto"
L1_DATASETS="dotgov govinfo"

# get the level datasets
for dataset in $L0_DATASETS; do
    echo "Pushing $dataset (L0)"
    uv run python3 kl3m_data/cli/pipeline.py push-to-hf "$dataset" "alea-institute/kl3m-data-$dataset" --temp-file-path "/data/$dataset.jsonl.gz"
done

# govinfo sublist
#┏━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━┓
#┃ Subfolder             ┃ Documents ┃ Representations ┃ Parquet ┃
#┡━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━┩
#│ BILLS                 │ ✓         │ ✓               │ ✓       │
#│ BUDGET                │ ✓         │ ✓               │ ✓       │
#│ CCAL                  │ ✓         │ ✓               │ ✓       │
#│ CDIR                  │ ✓         │ ✓               │ ✓       │
#│ CDOC                  │ ✓         │ ✓               │ ✓       │
#│ CFR                   │ ✓         │ ✓               │ ✓       │
#│ CHRG                  │ ✓         │ ✓               │ ✓       │
#│ CMR                   │ ✓         │ ✓               │ ✓       │
#│ COMPS                 │ ✓         │ ✓               │ ✓       │
#│ CPD                   │ ✓         │ ✓               │ ✓       │
#│ CPRT                  │ ✓         │ ✓               │ ✓       │
#│ CREC                  │ ✓         │ ✓               │ ✓       │
#│ CRECB                 │ ✓         │ ✓               │ ✓       │
#│ CRI                   │ ✓         │ ✓               │ ✓       │
#│ CRPT                  │ ✓         │ ✓               │ ✓       │
#│ CZIC                  │ ✓         │ ✓               │ ✓       │
#│ ECONI                 │ ✓         │ ✓               │ ✓       │
#│ ERIC                  │ ✓         │ ✓               │ ✓       │
#│ ERP;CDOC              │ ✓         │ ✓               │ ✓       │
#│ FR                    │ ✓         │ ✓               │ ✓       │
#│ GAOREPORTS            │ ✓         │ ✓               │ ✓       │
#│ GOVMAN                │ ✓         │ ✓               │ ✓       │
#│ GOVPUB                │ ✓         │ ✓               │ ✓       │
#│ GOVPUB;CHRG           │ ✓         │ ✓               │ ✓       │
#│ GPO                   │ ✓         │ ✓               │ ✓       │
#│ GPO;CDOC              │ ✓         │ ✓               │ ✓       │
#│ GPO;CFR               │ ✓         │ ✓               │ ✓       │
#│ GPO;CPRT              │ ✓         │ ✓               │ ✓       │
#│ GPO;CRECB             │ ✓         │ ✓               │ ✓       │
#│ GPO;CRPT              │ ✓         │ ✓               │ ✓       │
#│ GPO;FR                │ ✓         │ ✓               │ ✓       │
#│ GPO;SJOURNAL          │ ✓         │ ✓               │ ✓       │
#│ HJOURNAL              │ ✓         │ ✓               │ ✓       │
#│ HMAN;CDOC             │ ✓         │ ✓               │ ✓       │
#│ HOB                   │ ✓         │ ✓               │ ✓       │
#│ LSA                   │ ✓         │ ✓               │ ✓       │
#│ PAI                   │ ✓         │ ✓               │ ✓       │
#│ PLAW                  │ ✓         │ ✓               │ ✓       │
#│ PPP                   │ ✓         │ ✓               │ ✓       │
#│ SERIALSET;CDOC        │ ✓         │ ✓               │ ✓       │
#│ SERIALSET;CDOC;BUDGET │ ✓         │ ✓               │ ✓       │
#│ SERIALSET;CRPT        │ ✓         │ ✓               │ ✓       │
#│ SERIALSET;CRPT;ERP    │ ✓         │ ✓               │ ✓       │
#│ SERIALSET;HJOURNAL    │ ✓         │ ✓               │ ✓       │
#│ SERIALSET;SJOURNAL    │ ✓         │ ✓               │ ✓       │
#│ SMAN;CDOC             │ ✓         │ ✓               │ ✓       │
#│ STATUTE               │ ✓         │ ✓               │ ✓       │
#│ USCODE                │ ✓         │ ✓               │ ✓       │
#│ USCOURTS              │ ✓         │ ✓               │ ✓       │
#└───────────────────────┴───────────┴─────────────────┴─────────┘

# datasets are listed in scripts/govinfo_datasets.txt separated by newlines
uv run python3 kl3m_data/cli/pipeline.py push-to-hf \
  govinfo "alea-institute/kl3m-data-bills" \
  --key-prefix "BILLS"
  --temp-file-path "/data/govinfo-bills.jsonl.gz"

uv run python3 kl3m_data/cli/pipeline.py push-to-hf \
  govinfo "alea-institute/kl3m-data-budget" \
  --key-prefix "BUDGET"
  --temp-file-path "/data/govinfo-budget.jsonl.gz"

uv run python3 kl3m_data/cli/pipeline.py push-to-hf \
  govinfo "alea-institute/kl3m-data-ccal" \
  --key-prefix "CCAL"
  --temp-file-path "/data/govinfo-ccal.jsonl.gz"

uv run python3 kl3m_data/cli/pipeline.py push-to-hf \
  govinfo "alea-institute/kl3m-data-cdir" \
  --key-prefix "CDIR"
  --temp-file-path "/data/govinfo-cdir.jsonl.gz"

uv run python3 kl3m_data/cli/pipeline.py push-to-hf \
  govinfo "alea-institute/kl3m-data-cdoc" \
  --key-prefix "CDOC"
  --temp-file-path "/data/govinfo-cdoc.jsonl.gz"

uv run python3 kl3m_data/cli/pipeline.py push-to-hf \
  govinfo "alea-institute/kl3m-data-chrg" \
  --key-prefix "CHRG"
  --temp-file-path "/data/govinfo-chrg.jsonl.gz"

uv run python3 kl3m_data/cli/pipeline.py push-to-hf \
  govinfo "alea-institute/kl3m-data-cmr" \
  --key-prefix "CHRG"
  --temp-file-path "/data/govinfo-chrg.jsonl.gz"


# dotgov sublist
#┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━┓
#┃ Subfolder                    ┃ Documents ┃ Representations ┃ Parquet ┃
#┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━┩
#│ acl.gov                      │ ✓         │ ✓               │ ✓       │
#│ adr.gov                      │ ✓         │ ✓               │ ✓       │
#│ americorps.gov               │ ✓         │ ✓               │ ✓       │
#│ cafc.uscourts.gov            │ ✓         │ ✓               │ ✓       │
#│ cic.ndu.edu                  │ ✓         │ ✓               │ ✓       │
#│ clerk.house.gov              │ ✓         │ ✓               │ ✓       │
#│ consumer.gov                 │ ✓         │ ✓               │ ✓       │
#│ cops.usdoj.gov               │ ✓         │ ✓               │ ✓       │
#│ discover.dtic.mil            │ ✓         │ ✓               │ ✓       │
#│ dra.gov                      │ ✓         │ ✓               │ ✓       │
#│ eca.state.gov                │ ✓         │ ✓               │ ✓       │
#│ ed.gov                       │ ✓         │ ✓               │ ✓       │
#│ es.ndu.edu                   │ ✓         │ ✓               │ ✓       │
#│ health.gov                   │ ✓         │ ✓               │ ✓       │
#│ highways.dot.gov             │ ✓         │ ✓               │ ✓       │
#│ home.treasury.gov            │ ✓         │ ✓               │ ✓       │
#│ jfsc.ndu.edu                 │ ✓         │ ✓               │ ✓       │
#│ juvenilecouncil.ojp.gov      │ ✓         │ ✓               │ ✓       │
#│ minorityhealth.hhs.gov       │ ✓         │ ✓               │ ✓       │
#│ ncd.gov                      │ ✓         │ ✓               │ ✓       │
#│ nicic.gov                    │ ✓         │ ✓               │ ✓       │
#│ nij.ojp.gov                  │ ✓         │ ✓               │ ✓       │
#│ nmb.gov                      │ ✓         │ ✗               │ ✗       │
#│ npin.cdc.gov                 │ ✓         │ ✓               │ ✓       │
#│ oceanservice.noaa.gov        │ ✓         │ ✓               │ ✓       │
#│ ojp.gov                      │ ✓         │ ✓               │ ✓       │
#│ oldcc.gov                    │ ✓         │ ✓               │ ✓       │
#│ onrr.gov                     │ ✓         │ ✓               │ ✓       │
#│ osc.gov                      │ ✓         │ ✓               │ ✓       │
#│ postalinspectors.uspis.gov   │ ✓         │ ✓               │ ✓       │
#│ railroads.dot.gov            │ ✓         │ ✓               │ ✓       │
#│ rrb.gov                      │ ✓         │ ✓               │ ✓       │
#│ science.gov                  │ ✓         │ ✓               │ ✓       │
#│ stats.bls.gov                │ ✓         │ ✓               │ ✓       │
#│ studentaid.gov               │ ✓         │ ✓               │ ✓       │
#│ trade.gov                    │ ✓         │ ✓               │ ✓       │
#│ travel.state.gov             │ ✓         │ ✓               │ ✓       │
#│ treasury.gov                 │ ✓         │ ✓               │ ✓       │
#│ usun.usmission.gov           │ ✓         │ ✓               │ ✓       │
#│ www.abilityone.gov           │ ✓         │ ✓               │ ✓       │
#│ www.abmc.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.access-board.gov         │ ✓         │ ✓               │ ✓       │
#│ www.acf.hhs.gov              │ ✓         │ ✓               │ ✓       │
#│ www.achp.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.acquisition.gov          │ ✓         │ ✓               │ ✓       │
#│ www.acus.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.af.mil                   │ ✓         │ ✓               │ ✓       │
#│ www.afrc.af.mil              │ ✓         │ ✓               │ ✓       │
#│ www.afrh.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.africom.mil              │ ✓         │ ✓               │ ✓       │
#│ www.ahrq.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.alhurra.com              │ ✓         │ ✓               │ ✓       │
#│ www.ams.usda.gov             │ ✓         │ ✓               │ ✓       │
#│ www.aoc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.aphis.usda.gov           │ ✓         │ ✓               │ ✓       │
#│ www.arc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.archives.gov             │ ✓         │ ✓               │ ✓       │
#│ www.arctic.gov               │ ✓         │ ✓               │ ✓       │
#│ www.armfor.uscourts.gov      │ ✓         │ ✓               │ ✓       │
#│ www.army.mil                 │ ✓         │ ✓               │ ✓       │
#│ www.ars.usda.gov             │ ✓         │ ✓               │ ✓       │
#│ www.arts.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.atf.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.atsdr.cdc.gov            │ ✓         │ ✓               │ ✓       │
#│ www.bea.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.benefits.va.gov          │ ✓         │ ✓               │ ✗       │
#│ www.bia.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.bis.doc.gov              │ ✓         │ ✓               │ ✓       │
#│ www.bjs.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.blm.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.boem.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.bop.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.bpa.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.bsee.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.bts.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.cancer.gov               │ ✓         │ ✓               │ ✓       │
#│ www.cbo.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.cbp.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.cdc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.cem.va.gov               │ ✓         │ ✓               │ ✓       │
#│ www.census.gov               │ ✓         │ ✓               │ ✓       │
#│ www.centcom.mil              │ ✓         │ ✓               │ ✓       │
#│ www.cfa.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.cfo.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.cftc.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.chcoc.gov                │ ✓         │ ✓               │ ✓       │
#│ www.cia.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.cio.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.cisa.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.cit.uscourts.gov         │ ✓         │ ✓               │ ✓       │
#│ www.cms.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.commerce.gov             │ ✓         │ ✓               │ ✓       │
#│ www.commissaries.com         │ ✓         │ ✓               │ ✓       │
#│ www.consumer.gov             │ ✓         │ ✓               │ ✓       │
#│ www.consumerfinance.gov      │ ✓         │ ✓               │ ✓       │
#│ www.copyright.gov            │ ✓         │ ✓               │ ✓       │
#│ www.cpsc.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.csb.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.csce.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.csosa.gov                │ ✓         │ ✓               │ ✓       │
#│ www.cybercom.mil             │ ✓         │ ✓               │ ✓       │
#│ www.darpa.mil                │ ✓         │ ✓               │ ✓       │
#│ www.dcaa.mil                 │ ✓         │ ✓               │ ✓       │
#│ www.dcma.mil                 │ ✓         │ ✓               │ ✓       │
#│ www.dcsa.mil                 │ ✓         │ ✓               │ ✓       │
#│ www.dea.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.defense.gov              │ ✓         │ ✓               │ ✓       │
#│ www.defenselink.mil          │ ✓         │ ✓               │ ✓       │
#│ www.denali.gov               │ ✓         │ ✓               │ ✓       │
#│ www.dfas.mil                 │ ✓         │ ✓               │ ✓       │
#│ www.dfc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.dhs.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.dia.mil                  │ ✓         │ ✓               │ ✓       │
#│ www.disa.mil                 │ ✓         │ ✓               │ ✓       │
#│ www.dla.mil                  │ ✓         │ ✓               │ ✓       │
#│ www.dnfsb.gov                │ ✓         │ ✓               │ ✓       │
#│ www.dni.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.doi.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.dol.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.doleta.gov               │ ✓         │ ✓               │ ✓       │
#│ www.dpaa.mil                 │ ✓         │ ✓               │ ✓       │
#│ www.dsca.mil                 │ ✓         │ ✓               │ ✓       │
#│ www.dtra.mil                 │ ✓         │ ✓               │ ✓       │
#│ www.eac.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.ed.gov                   │ ✓         │ ✓               │ ✓       │
#│ www.eda.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.eeoc.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.eia.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.energy.gov               │ ✓         │ ✓               │ ✓       │
#│ www.epa.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.ers.usda.gov             │ ✓         │ ✓               │ ✓       │
#│ www.eucom.mil                │ ✓         │ ✓               │ ✓       │
#│ www.exim.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.faa.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.fanniemae.com            │ ✓         │ ✓               │ ✓       │
#│ www.fas.usda.gov             │ ✓         │ ✓               │ ✓       │
#│ www.fasab.gov                │ ✓         │ ✓               │ ✓       │
#│ www.fbi.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.fca.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.fcc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.fcsic.gov                │ ✓         │ ✓               │ ✓       │
#│ www.fcsm.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.fda.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.fdic.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.feb.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.fec.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.federalreserve.gov       │ ✓         │ ✓               │ ✓       │
#│ www.fema.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.ferc.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.fgdc.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.fhfa.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.firescience.gov          │ ✓         │ ✓               │ ✓       │
#│ www.fiscal.treasury.gov      │ ✓         │ ✓               │ ✓       │
#│ www.fjc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.fletc.gov                │ ✓         │ ✓               │ ✓       │
#│ www.floodsmart.gov           │ ✓         │ ✓               │ ✓       │
#│ www.flra.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.fmc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.fmcs.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.fmcsa.dot.gov            │ ✓         │ ✓               │ ✓       │
#│ www.fmshrc.gov               │ ✓         │ ✓               │ ✓       │
#│ www.fns.usda.gov             │ ✓         │ ✓               │ ✓       │
#│ www.freddiemac.com           │ ✓         │ ✓               │ ✓       │
#│ www.frtib.gov                │ ✓         │ ✓               │ ✓       │
#│ www.fs.usda.gov              │ ✓         │ ✓               │ ✓       │
#│ www.fsa.usda.gov             │ ✓         │ ✓               │ ✓       │
#│ www.fsis.usda.gov            │ ✓         │ ✓               │ ✓       │
#│ www.ftc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.fvap.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.fws.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.gao.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.ginniemae.gov            │ ✓         │ ✓               │ ✓       │
#│ www.gpo.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.gsa.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.health.mil               │ ✓         │ ✓               │ ✓       │
#│ www.hhs.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.house.gov                │ ✓         │ ✓               │ ✓       │
#│ www.hrsa.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.hud.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.huduser.gov              │ ✓         │ ✓               │ ✓       │
#│ www.iaf.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.ice.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.ignet.gov                │ ✓         │ ✓               │ ✓       │
#│ www.ihs.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.imls.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.inaugural.senate.gov     │ ✓         │ ✓               │ ✓       │
#│ www.insidevoa.com            │ ✓         │ ✓               │ ✓       │
#│ www.invasivespeciesinfo.gov  │ ✓         │ ✓               │ ✓       │
#│ www.irs.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.jamesmadison.gov         │ ✓         │ ✓               │ ✓       │
#│ www.jcs.mil                  │ ✓         │ ✓               │ ✓       │
#│ www.jobcorps.gov             │ ✓         │ ✓               │ ✓       │
#│ www.jpeocbrnd.osd.mil        │ ✓         │ ✓               │ ✓       │
#│ www.jpml.uscourts.gov        │ ✓         │ ✓               │ ✓       │
#│ www.jusfc.gov                │ ✓         │ ✗               │ ✗       │
#│ www.justice.gov              │ ✓         │ ✓               │ ✓       │
#│ www.lsc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.macpac.gov               │ ✓         │ ✓               │ ✓       │
#│ www.marines.mil              │ ✓         │ ✓               │ ✓       │
#│ www.maritime.dot.gov         │ ✓         │ ✓               │ ✓       │
#│ www.mbda.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.mcc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.mda.mil                  │ ✓         │ ✓               │ ✓       │
#│ www.medpac.gov               │ ✓         │ ✓               │ ✓       │
#│ www.mmc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.moneyfactory.gov         │ ✓         │ ✓               │ ✓       │
#│ www.msha.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.mspb.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.mvd.usace.army.mil       │ ✓         │ ✓               │ ✓       │
#│ www.nal.usda.gov             │ ✓         │ ✓               │ ✓       │
#│ www.nasa.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.nass.usda.gov            │ ✓         │ ✓               │ ✓       │
#│ www.nationalguard.mil        │ ✓         │ ✓               │ ✓       │
#│ www.nationalparks.org        │ ✓         │ ✓               │ ✓       │
#│ www.navy.mil                 │ ✓         │ ✓               │ ✓       │
#│ www.nbrc.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.nccih.nih.gov            │ ✓         │ ✓               │ ✓       │
#│ www.ncpc.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.ncua.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.ndu.edu                  │ ✓         │ ✓               │ ✓       │
#│ www.neh.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.nga.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.nga.mil                  │ ✓         │ ✓               │ ✓       │
#│ www.nhlbi.nih.gov            │ ✓         │ ✓               │ ✓       │
#│ www.nhtsa.gov                │ ✓         │ ✓               │ ✓       │
#│ www.ni-u.edu                 │ ✓         │ ✓               │ ✓       │
#│ www.niaid.nih.gov            │ ✓         │ ✓               │ ✓       │
#│ www.niams.nih.gov            │ ✓         │ ✓               │ ✓       │
#│ www.nidcd.nih.gov            │ ✓         │ ✓               │ ✓       │
#│ www.niddk.nih.gov            │ ✓         │ ✓               │ ✓       │
#│ www.nifa.usda.gov            │ ✓         │ ✓               │ ✓       │
#│ www.nifc.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.nigc.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.nih.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.nimh.nih.gov             │ ✓         │ ✓               │ ✓       │
#│ www.ninds.nih.gov            │ ✓         │ ✓               │ ✓       │
#│ www.nist.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.nlm.nih.gov              │ ✓         │ ✓               │ ✓       │
#│ www.nlrb.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.nmfs.noaa.gov            │ ✓         │ ✓               │ ✓       │
#│ www.noaa.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.northcom.mil             │ ✓         │ ✓               │ ✓       │
#│ www.nps.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.nrc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.nrcs.usda.gov            │ ✓         │ ✓               │ ✓       │
#│ www.nro.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.nsa.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.nsf.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.ntia.doc.gov             │ ✓         │ ✓               │ ✓       │
#│ www.ntis.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.ntsb.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.nws.noaa.gov             │ ✓         │ ✓               │ ✓       │
#│ www.nwtrb.gov                │ ✓         │ ✓               │ ✓       │
#│ www.occ.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.ocwr.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.oge.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.ojjdp.gov                │ ✓         │ ✓               │ ✓       │
#│ www.openworld.gov            │ ✓         │ ✓               │ ✓       │
#│ www.opm.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.ornl.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.osha.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.oshrc.gov                │ ✓         │ ✓               │ ✓       │
#│ www.osmre.gov                │ ✓         │ ✓               │ ✓       │
#│ www.osti.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.pacom.mil                │ ✓         │ ✓               │ ✓       │
#│ www.pbgc.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.pclob.gov                │ ✓         │ ✓               │ ✓       │
#│ www.pfpa.mil                 │ ✓         │ ✓               │ ✓       │
#│ www.phmsa.dot.gov            │ ✓         │ ✓               │ ✓       │
#│ www.prc.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.presidio.gov             │ ✓         │ ✓               │ ✓       │
#│ www.psa.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.radiotelevisionmarti.com │ ✓         │ ✓               │ ✓       │
#│ www.rd.usda.gov              │ ✓         │ ✓               │ ✓       │
#│ www.rma.usda.gov             │ ✓         │ ✓               │ ✓       │
#│ www.samhsa.gov               │ ✓         │ ✓               │ ✓       │
#│ www.sba.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.science.gov              │ ✓         │ ✓               │ ✓       │
#│ www.seaway.dot.gov           │ ✓         │ ✓               │ ✓       │
#│ www.sec.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.secretservice.gov        │ ✓         │ ✓               │ ✓       │
#│ www.senate.gov               │ ✓         │ ✓               │ ✓       │
#│ www.si.edu                   │ ✓         │ ✓               │ ✓       │
#│ www.sji.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.socom.mil                │ ✓         │ ✓               │ ✓       │
#│ www.southcom.mil             │ ✓         │ ✓               │ ✓       │
#│ www.spacecom.mil             │ ✓         │ ✓               │ ✓       │
#│ www.srbc.net                 │ ✓         │ ✓               │ ✓       │
#│ www.ssa.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.ssab.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.sss.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.state.gov                │ ✓         │ ✓               │ ✓       │
#│ www.state.nj.us              │ ✓         │ ✓               │ ✓       │
#│ www.stb.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.stennis.gov              │ ✓         │ ✓               │ ✓       │
#│ www.stratcom.mil             │ ✓         │ ✓               │ ✓       │
#│ www.supremecourtus.gov       │ ✓         │ ✓               │ ✓       │
#│ www.trade.gov                │ ✓         │ ✓               │ ✓       │
#│ www.transit.dot.gov          │ ✓         │ ✓               │ ✓       │
#│ www.transportation.gov       │ ✓         │ ✓               │ ✓       │
#│ www.treas.gov                │ ✓         │ ✓               │ ✓       │
#│ www.treasury.gov             │ ✓         │ ✓               │ ✓       │
#│ www.truman.gov               │ ✓         │ ✓               │ ✓       │
#│ www.tsa.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.ttb.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.tva.com                  │ ✓         │ ✓               │ ✓       │
#│ www.udall.gov                │ ✓         │ ✓               │ ✓       │
#│ www.usa.gov                  │ ✓         │ ✓               │ ✓       │
#│ www.usace.army.mil           │ ✓         │ ✓               │ ✓       │
#│ www.usadf.gov                │ ✓         │ ✓               │ ✓       │
#│ www.usagm.gov                │ ✓         │ ✓               │ ✓       │
#│ www.usaid.gov                │ ✓         │ ✓               │ ✓       │
#│ www.usbg.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.usbr.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.usccr.gov                │ ✓         │ ✓               │ ✓       │
#│ www.uscfc.uscourts.gov       │ ✓         │ ✓               │ ✓       │
#│ www.uscg.mil                 │ ✓         │ ✓               │ ✓       │
#│ www.uscirf.gov               │ ✓         │ ✓               │ ✓       │
#│ www.uscis.gov                │ ✓         │ ✓               │ ✓       │
#│ www.uscourts.cavc.gov        │ ✓         │ ✓               │ ✓       │
#│ www.uscourts.gov             │ ✓         │ ✓               │ ✓       │
#│ www.uscp.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.usda.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.usdoj.gov                │ ✓         │ ✓               │ ✓       │
#│ www.usfa.fema.gov            │ ✓         │ ✓               │ ✓       │
#│ www.usff.navy.mil            │ ✓         │ ✓               │ ✓       │
#│ www.usgs.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.usich.gov                │ ✓         │ ✓               │ ✓       │
#│ www.usmint.gov               │ ✓         │ ✓               │ ✓       │
#│ www.uspto.gov                │ ✓         │ ✓               │ ✓       │
#│ www.ussc.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.ustaxcourt.gov           │ ✓         │ ✓               │ ✓       │
#│ www.ustda.gov                │ ✓         │ ✓               │ ✓       │
#│ www.ustr.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.usuhs.edu                │ ✓         │ ✓               │ ✓       │
#│ www.va.gov                   │ ✓         │ ✓               │ ✓       │
#│ www.visitthecapitol.gov      │ ✓         │ ✓               │ ✓       │
#│ www.wapa.gov                 │ ✓         │ ✓               │ ✓       │
#│ www.westpoint.edu            │ ✓         │ ✓               │ ✓       │
#│ www.whitehouse.gov           │ ✓         │ ✓               │ ✓       │
#│ www.whs.mil                  │ ✓         │ ✓               │ ✓       │
#│ www2.ed.gov                  │ ✓         │ ✓               │ ✓       │
#│ youth.gov                    │ ✓         │ ✓               │ ✓       │
#└──────────────────────────────┴───────────┴─────────────────┴─────────┘

# readline again
while IFS= read -r dataset; do
    echo "Updating counts for $dataset (dotgov)"
    uv run python3 kl3m_data/cli/pipeline.py substatus dotgov "$dataset" --csv "stats/$dataset.csv"
done < scripts/dotgov_datasets.txt