
import json
import os
from typing import Any, Iterable

def JSONLDictGen(file_loc: str) -> Iterable[dict[str,Any]]:
    with open(file_loc) as edgejsonl:
        for row in edgejsonl:
            d: dict[str, Any] = json.loads(row)
            yield d

def main():
    tmpk_edge_jsonl = os.getenv("TMKP_EDGES")
    if(tmpk_edge_jsonl==None): raise ValueError("Need to set $TMKP_EDGES to the location of the edge file you want to count.")
    #This is a dictonary to keep track of the various different 
    # types of publications we can find in a TMKP file.
    # We can find three types of publications in two places.
    # We can find a full paper (a PMC or PMID with full release),
    # an abstract (a PMID without a full release), or unknown (a PMID which we cannot determine from the data here 
    # what it is; these are likely abstracts.)
    tmkp_cnts:dict[str,int] = {
        "abst_kgx":0, 
        "publ_kgx":0,
        "unknown_kgx":0,
        "abst_xref":0,
        "publ_xref":0,
    }
    tmkp_uniques:dict[str,set[str]] = {
        "abst_kgx":set[str](), 
        "publ_kgx":set[str](),
        "unknown_kgx":set[str](),
        "abst_xref":set[str](),
        "publ_xref":set[str](),
    }
    #This first pass goes through all of the xrefs and gathers data from them.
    for edge_dict in JSONLDictGen(tmpk_edge_jsonl):
        for study in edge_dict["has_supporting_studies"].values():
            for study_result in study["has_study_results"]:
                text_type = study_result["supporting_text_section_type"]
                #*text_type* of ABSTRACT or TITLE means it's a fulltext paper, lowercase mean just abstract.
                is_abstract = "abstract" in text_type or "title" in text_type
                if(len(study_result["xref"])!=1):raise ValueError(f"Weird xref in the following jsonl line---halting---\n{edge_dict}")
                xref = study_result["xref"][0]
                if(is_abstract):
                    tmkp_uniques["abst_xref"].add(xref)
                    tmkp_cnts["abst_xref"]+=1
                else: 
                    tmkp_uniques["publ_xref"].add(xref)
                    tmkp_cnts["publ_xref"]+=1

    tmkp_edge_cnt = 0
    for edge_dict in JSONLDictGen(tmpk_edge_jsonl):
        tmkp_edge_cnt+=1
        for pub in edge_dict["publications"]:
            is_publication = ("PMC" in pub) or (pub in tmkp_uniques["publ_xref"])
            is_abstract = (pub in tmkp_uniques["abst_xref"])
            if(is_publication):
                tmkp_uniques["publ_kgx"].add(pub)
                tmkp_cnts["publ_kgx"]+=1
            elif(is_abstract):
                tmkp_uniques["abst_kgx"].add(pub)
                tmkp_cnts["abst_kgx"]+=1
            else:
                tmkp_uniques["unknown_kgx"].add(pub)
                tmkp_cnts["unknown_kgx"]+=1
    
    
    output_headers = {"abst_kgx":"ABSTRACTS","publ_kgx":"FULL PUBLICATIONS","unknown_kgx":"CAN'T TELL"}
    for key in ["abst_kgx","publ_kgx","unknown_kgx"]:
        output_str = f"--- {output_headers[key]} ---\n" +\
                     f"Found {len(tmkp_uniques[key])} unique publications in this category\n" +\
                     f"Found {tmkp_cnts[key]} supported relationships by this category\n" +\
                     f"5 Samples from this category --- {', '.join(sorted(list(tmkp_uniques[key]))[0:5])}"
        print(output_str)
    
    total_uniq_pubs = sum([len(tmkp_uniques[k]) for k in ["abst_kgx","publ_kgx","unknown_kgx"]])
    total_rels = sum([(tmkp_cnts[k]) for k in ["abst_kgx","publ_kgx","unknown_kgx"]])
    output_str = "--- TOTAL ---\n" +\
                     f"{tmkp_edge_cnt} relationships iterated through in TMKP\n" +\
                     f"{total_uniq_pubs} unique publications found\n" +\
                     f"{total_rels} supported relationships"    
    print(output_str)

if(__name__=="__main__"):
    main()
