#!/usr/bin/env python
# ce_rank.py  – CareersEdge undergraduate ranking engine (v5.2)

from __future__ import annotations
import argparse, random, logging, sys
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
from tabulate import tabulate

# optional libs
try:
    import yaml
except ImportError:
    yaml = None
try:
    from sklearn.linear_model import LogisticRegression
except ImportError:
    LogisticRegression = None
try:
    import matplotlib.pyplot as plt
except ImportError:
    plt = None
try:
    from fastapi import FastAPI
    from pydantic import BaseModel
    import uvicorn
except ImportError:
    FastAPI = uvicorn = None

# ────────────────────────────────────────────────────────────────
# 1.  CONFIG & CONSTANTS
# ────────────────────────────────────────────────────────────────
DECAY_RATE, DECAY_FLOOR = 0.10, 0.50
INTERN_TIER_PTS = {1: 100, 2: 80, 3: 60}
SOCIETY_ROLE_PTS = {"president": 100, "vice": 80, "committee": 60, "member": 40}

CFG: Dict[str, Any] = {
    "academic_weights": {
        0: {"pts": 25, "alevel": .50, "gcse": .30, "awards": .15, "certs": .05},
        1: {"pts": 30, "university": .30, "alevel": .30, "gcse": .15,
            "awards": .15, "certs": .10},
        2: {"pts": 40, "grades": .40, "university": .25, "awards": .20,
            "certs": .10, "alevel": .04, "gcse": .01},
        3: {"pts": 45, "grades": .45, "university": .20, "awards": .20,
            "certs": .10, "alevel": .03, "gcse": .01},
    },
    "experience_caps": {0: 30, 1: 70, 2: 60, 3: 55},
    "intern_mix": {"alpha": 0.8, "beta": 0.2},  # α for internships, β for society
    "star_bands": [[80, "★★★★★"], [60, "★★★★☆"], [40, "★★★☆☆"],
                   [20, "★★☆☆☆"], [0, "★☆☆☆☆"]],
    "lookups": {
        "uni_y1": {"Oxford": 30, "Cambridge": 30, "LSE": 30, "Imperial": 30,
                   "Warwick": 28, "Non-Target": 20},
        "uni_y2": {"Oxford": 25, "Cambridge": 25, "LSE": 25, "Imperial": 25,
                   "Warwick": 23, "Non-Target": 17},
        "uni_y3": {"Oxford": 20, "Cambridge": 20, "LSE": 20, "Imperial": 20,
                   "Warwick": 18, "Non-Target": 10},
        "alevel": {"AAA*": 30, "A*AA": 28, "AAA": 24, "AAB": 22, "ABB": 20,
                   "BBB": 18, "BBC": 12, "Others": 8},
        "gcse": {"8+7-9": 2.0, "6-7 7-9": 1.8, "4-5 7-9": 1.5,
                 "Majority 6": 1.2, "Pass": 0.8, "Below": 0.2},
        "grade_y2": {"80+": 40, "70-79": 36, "60-69": 32,
                     "55-59": 24, "50-54": 20, "<50": 0},
        "grade_y3": {"80+": 45, "73-79": 43, "70-72": 41,
                     "66-69": 38, "62-65": 36, "59-61": 34,
                     "55-59": 28, "50-54": 24, "40-49": 12, "<40": 0},
        "bank_tier": {"Bulge": 5, "Elite": 5, "Mid": 4, "UpperMid": 4,
                      "LowerMid": 3, "Boutique": 2, "N/A": 0},
        "exposure": {"Direct": 5, "Related": 3, "General": 1, "None": 0},
    }
}

def deep_update(base: dict, upd: dict) -> dict:
    for k, v in upd.items():
        base[k] = deep_update(base.get(k, {}), v) if isinstance(v, dict) else v
    return base


# ────────────────────────────────────────────────────────────────
# 2.  LOGGING
# ────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("ce-rank")


# ────────────────────────────────────────────────────────────────
# 3.  SCORER
# ────────────────────────────────────────────────────────────────
class Scorer:
    def __init__(self, cfg: Dict = CFG):
        self.cfg = cfg
        self.aw = pd.DataFrame(cfg["academic_weights"]).T
        self.cap = cfg["experience_caps"]
        self.mix = cfg["intern_mix"]
        self.lu = cfg["lookups"]
        self.bands = sorted(cfg["star_bands"], key=lambda x: -x[0])

    def _decay(self, years: int) -> float:
        return max(1 - DECAY_RATE * years, DECAY_FLOOR)

    def _academic(self, df: pd.DataFrame) -> pd.Series:
        out = pd.Series(0.0, index=df.index); L = self.lu
        for y, grp in df.groupby("year"):
            w = self.aw.loc[y]; pts = pd.Series(0.0, index=grp.index)
            if y == 0:
                pts += w.alevel * grp["alevel"].map(L["alevel"]) / 30
                pts += w.gcse   * grp["gcse"].map(L["gcse"])   / 2
            elif y == 1:
                pts += w.university * grp["university"].map(L["uni_y1"]).fillna(20) / 30
                pts += w.alevel     * grp["alevel"].map(L["alevel"]) / 30
                pts += w.gcse       * grp["gcse"].map(L["gcse"])   / 2
            elif y == 2:
                pts += w.grades     * grp["grades"].map(L["grade_y2"]) / 40
                pts += w.university * grp["university"].map(L["uni_y2"]).fillna(17) / 25
                pts += w.alevel     * grp["alevel"].map(L["alevel"]) / 30
            else:
                pts += w.grades     * grp["grades"].map(L["grade_y3"]) / 45
                pts += w.university * grp["university"].map(L["uni_y3"]).fillna(10) / 20
                pts += w.alevel     * grp["alevel"].map(L["alevel"]) / 30
            pts += w.awards * grp["awards"] / 15
            pts += w.certs  * grp["certs"]  / 10
            out.loc[grp.index] = pts * w.pts
        return out

    def _base_experience(self, df: pd.DataFrame) -> pd.Series:
        L = self.lu
        prestige = df["bank_tier"].map(L["bank_tier"]).fillna(0)
        exposure = df["exposure"].map(L["exposure"]).fillna(0)
        raw = (prestige + exposure) * df["months"].clip(lower=0) / 3
        return raw * df["year"].map(self.cap) / 100

    def _internship(self, lst: List[dict]) -> float:
        if not lst: return 0.0
        total = 0.0; months = 0
        for i in lst:
            tier, mth, yrs = i["tier"], i["months"], i["years"]
            pts = INTERN_TIER_PTS.get(tier, 0)
            total += pts * mth * self._decay(yrs)
            months += mth
        return total / months if months else 0.0

    def _society(self, lst: List[dict]) -> float:
        if not lst: return 0.0
        vals = []
        for p in lst:
            base = SOCIETY_ROLE_PTS.get(p["role"], 0)
            size_factor = (p["size"] + 1)**0.5 / 10
            vals.append(base * size_factor * self._decay(p["years"]))
        return sum(vals) / len(vals)

    def _experience(self, df: pd.DataFrame) -> pd.Series:
        base = self._base_experience(df)
        intern = df["internships"].apply(self._internship)
        soc    = df["society"].apply(self._society)
        return base + self.mix["alpha"] * intern + self.mix["beta"] * soc

    def _star(self, pct: float) -> str:
        for cut, sym in self.bands:
            if pct >= cut:
                return sym
        return self.bands[-1][1]

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["Academic"]   = self._academic(out)
        out["Experience"] = self._experience(out)
        out["Composite"]  = out["Academic"] + out["Experience"]
        out["Index"]      = out.groupby("year")["Composite"].rank("average", pct=True) * 100
        out["Stars"]      = out["Index"].apply(self._star)
        out.sort_values(["Index","Composite","Academic"],
                        ascending=[False,False,False], inplace=True)
        out.insert(0, "Rank", range(1, len(out)+1))
        return out.round(2)


# ────────────────────────────────────────────────────────────────
# 4.  DATA GEN & VALIDATION
# ────────────────────────────────────────────────────────────────
PY_UNIS = list(CFG["lookups"]["uni_y1"]) + ["Non-Target"]
PY_BANK = list(CFG["lookups"]["bank_tier"])
PY_EXP  = list(CFG["lookups"]["exposure"])
PY_ALEV = list(CFG["lookups"]["alevel"])
PY_GCSE = list(CFG["lookups"]["gcse"])
PY_Y2   = list(CFG["lookups"]["grade_y2"])
PY_Y3   = list(CFG["lookups"]["grade_y3"])

def random_student(i: int) -> Dict:
    y = random.choice([0,1,2,3])
    return {
        "ID": f"S{i:03}", "year": y,
        "university": random.choice(PY_UNIS) if y else "N/A",
        "alevel": random.choice(PY_ALEV), "gcse": random.choice(PY_GCSE),
        "grades": random.choice(PY_Y2) if y==2 else (random.choice(PY_Y3) if y==3 else "N/A"),
        "awards": random.randint(0,20), "certs": random.randint(0,10),
        "bank_tier": random.choice(PY_BANK), "exposure": random.choice(PY_EXP),
        "months": random.choice([0,1,2,3,6]),
        "internships": [
            {"tier": random.randint(1,3),
             "months": random.randint(2,12),
             "years": random.randint(0,2)}
            for _ in range(random.randint(0,2))
        ],
        "society": (
            [{"role": random.choice(list(SOCIETY_ROLE_PTS)),
              "size": random.randint(10,120),
              "years": random.randint(0,2)}]
            if random.random()<0.5 else []
        )
    }

def synth(n: int, seed: int|None=None) -> pd.DataFrame:
    random.seed(seed)
    return pd.DataFrame(random_student(i) for i in range(1, n+1))

def validate(df: pd.DataFrame):
    req = {"ID","year","university","alevel","gcse","grades",
           "awards","certs","bank_tier","exposure","months",
           "internships","society"}
    miss = req - set(df.columns)
    if miss:
        sys.exit(f"Missing columns: {miss}")
    # warn unknown categories
    LK = CFG["lookups"]
    for col,key in [("alevel","alevel"),("gcse","gcse"),
                    ("bank_tier","bank_tier"),("exposure","exposure")]:
        bad = set(df[col]) - set(LK[key])
        if bad:
            log.warning("Unknown %s values ignored: %s", col, bad)


# ────────────────────────────────────────────────────────────────
# 5.  CLI / MAIN
# ────────────────────────────────────────────────────────────────
def cli():
    p = argparse.ArgumentParser("ce_rank")
    p.add_argument("-n","--num",    type=int,   default=80)
    p.add_argument("--in",          dest="infile",    type=Path)
    p.add_argument("--out",         dest="outfile",   type=Path, default=Path("ranked.csv"))
    p.add_argument("--excel",       type=Path)
    p.add_argument("--chart",       type=Path)
    p.add_argument("--summary",     action="store_true")
    p.add_argument("--split-out",   dest="splitdir",  type=Path)
    p.add_argument("--self-test",   action="store_true")
    p.add_argument("--config",      type=Path)
    p.add_argument("--train",       type=Path)
    p.add_argument("--save-config", type=Path)
    p.add_argument("--serve",       type=int)
    return p.parse_args()

def main():
    args = cli()

    # config override
    cfg = CFG.copy()
    if args.config:
        if not yaml: sys.exit("pyyaml required for --config")
        cfg = deep_update(cfg, yaml.safe_load(args.config.read_text()))

    scorer = Scorer(cfg)

    # ML training stub
    if args.train:
        df_t = pd.read_csv(args.train)
        if not all(c in df_t for c in ("Academic","Experience","offer")):
            sys.exit("Training CSV needs Academic, Experience, offer")
        clf = LogisticRegression(max_iter=500).fit(df_t[["Academic","Experience"]], df_t["offer"])
        log.info("ML weights learned: %s", clf.coef_)
        if args.save_config and yaml:
            new = deep_update(CFG.copy(), {"ml_blend": dict(phase1=clf.coef_[0][0], phase2=clf.coef_[0][1])})
            args.save_config.write_text(yaml.safe_dump(new))
            log.info("Saved tuned config → %s", args.save_config)
        return

    # build data
    df_raw = synth(12,42) if args.self_test else (pd.read_csv(args.infile) if args.infile else synth(args.num))
    validate(df_raw)

    ranked = scorer.score(df_raw)

    # CSV
    ranked.to_csv(args.outfile, index=False)
    log.info("CSV → %s", args.outfile)

    # Excel
    if args.excel:
        with pd.ExcelWriter(args.excel) as w:
            ranked.to_excel(w, sheet_name="National", index=False)
            for y, grp in ranked.groupby("year"):
                grp.to_excel(w, sheet_name=f"Year{y}", index=False)
        log.info("Excel → %s", args.excel)

    # chart
    if args.chart:
        if plt:
            plt.figure(); ranked["Composite"].hist(bins=20)
            plt.title("Composite score distribution"); plt.tight_layout()
            plt.savefig(args.chart, dpi=300)
            log.info("Chart → %s", args.chart)
        else:
            log.warning("matplotlib missing – chart skipped")

    # summary
    if args.summary:
        print("\n--- Summary ---")
        print(ranked[["Composite","Index"]].describe().round(2))
        print("\nStar counts:\n", ranked["Stars"].value_counts())

    # split
    if args.splitdir:
        args.splitdir.mkdir(exist_ok=True)
        for y, grp in ranked.groupby("year"):
            path = args.splitdir / f"year{y}.csv"
            grp.to_csv(path, index=False)
            log.info("Wrote %s", path)

    # preview
    print("\n### National Top 15\n")
    print(tabulate(ranked.head(15), headers="keys", tablefmt="github", showindex=False))

    # serve API
    if args.serve:
        if not FastAPI: sys.exit("fastapi/uvicorn required for --serve")
        app = FastAPI(title="CareersEdge Rank API")
        class Row(BaseModel):
            ID: str; year: int; university: str = "N/A"
            alevel: str; gcse: str; grades: str = "N/A"
            awards: int = 0; certs: int = 0
            bank_tier: str = "N/A"; exposure: str = "None"; months: int = 0
            internships: List[dict]; society: List[dict]
        @app.post("/rank")
        def rank(rows: List[Row]):
            df = pd.DataFrame([r.dict() for r in rows])
            validate(df)
            return scorer.score(df).to_dict(orient="records")
        uvicorn.run(app, host="0.0.0.0", port=args.serve)

if __name__ == "__main__":
    main()
