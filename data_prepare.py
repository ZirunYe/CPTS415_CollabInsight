import ijson
import csv
import time

json_file = "dblp.v12.json"
output_csv = "dblp.csv"

def clean_str(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip() # remove space before and after value
    return str(value)

start = time.time()
count = 0

with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = [
        "paperId", # must not be null
        "authorId",
        "authorName",
        "title",
        "year",
        "n_citation",
        "doc_type",
        "reference_count",
        "references",
        "doi"
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    with open(json_file, "rb") as f:
        for paper in ijson.items(f, "item"):
            count += 1

            paper_id = paper.get("id")
            if paper_id is None:
                continue

            title = clean_str(paper.get("title"))
            year = clean_str(paper.get("year"))
            n_citation = clean_str(paper.get("n_citation"))
            doc_type = clean_str(paper.get("doc_type"))
            doi = clean_str(paper.get("doi"))

            references = paper.get("references", [])
            reference_count = len(references) if references else 0
            references_str = ",".join(clean_str(str(r)) for r in references) if references else ""

            authors = paper.get("authors", [])
            if not authors:
                writer.writerow({
                    "paperId": paper_id,
                    "authorId": "",
                    "authorName": "",
                    "title": title,
                    "year": year,
                    "n_citation": n_citation,
                    "doc_type": doc_type,
                    "reference_count": reference_count,
                    "references": references_str,
                    "doi": doi
                })
            else:
                for author in authors:
                    author_id = clean_str(author.get("id"))
                    author_name = clean_str(author.get("name"))
                    writer.writerow({
                        "paperId": paper_id,
                        "authorId": author_id,
                        "authorName": author_name,
                        "title": title,
                        "year": year,
                        "n_citation": n_citation,
                        "doc_type": doc_type,
                        "reference_count": reference_count,
                        "references": references_str,
                        "doi": doi
                    })

            if count % 50000 == 0:
                print(f"{count} done")

elapsed = round(time.time() - start, 2)
print(f"Done.  {count} papers in {elapsed} s")