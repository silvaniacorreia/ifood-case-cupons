setup:
	pip install -r requirements.txt

notebooks:
	jupyter notebook

test:
	pytest tests/

 report:
	jupyter nbconvert --to webpdf notebooks/03_financial_roi.ipynb --output report/ifood_case_relatorio.pdf

report_html:
	jupyter nbconvert --to html notebooks/03_financial_roi.ipynb --output report/ifood_case_relatorio.html

data:
	python scripts/download_data.py