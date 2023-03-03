from pypdf import PdfMerger
import os
caminho = r'C:\Users\ABRASEL NACIONAL\Desktop\mkdocs/'
pdfs = list(filter(lambda x: '.pdf' in x, os.listdir(caminho)))
pdfs = sorted(pdfs)

merger = PdfMerger()

for pdf in pdfs:
    merger.append(f'{caminho}{pdf}')
    print(pdf)
merger.write(f"{caminho}DocumentaçãoMKDocsA3Data.pdf")
merger.close()