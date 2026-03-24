import zipfile
import xml.etree.ElementTree as ET
import glob

def extract_first_rows(xlsx_file, num_rows=25):
    try:
        with zipfile.ZipFile(xlsx_file, 'r') as z:
            shared_strings = []
            if 'xl/sharedStrings.xml' in z.namelist():
                xml_content = z.read('xl/sharedStrings.xml')
                root = ET.fromstring(xml_content)
                # handle namespaces by stripping them or using wildcard
                for t in root.iter():
                    if t.tag.endswith('}t'):
                        shared_strings.append(t.text if t.text else "")
                        
            sheet_xml = None
            for name in z.namelist():
                if name.startswith('xl/worksheets/sheet1.xml'):
                    sheet_xml = z.read(name)
                    break
            
            if not sheet_xml:
                print(f"{xlsx_file}: No sheet1.xml found.\\n")
                return
                
            root = ET.fromstring(sheet_xml)
            print(f"\\n{'='*50}\\nFILE: {xlsx_file}\\n{'='*50}")
            row_count = 0
            
            for row in root.iter():
                if row.tag.endswith('}row'):
                    if row_count >= num_rows: break
                    row_vals = []
                    for c in row:
                        if c.tag.endswith('}c'):
                            v = None
                            for child in c:
                                if child.tag.endswith('}v'):
                                    v = child
                            if v is not None and v.text is not None:
                                val = v.text
                                if c.get('t') == 's':
                                    val = shared_strings[int(val)] if int(val) < len(shared_strings) else val
                                row_vals.append(str(val))
                            else:
                                row_vals.append("")
                    print(f"Row {row.get('r')}: {' | '.join(row_vals)}")
                    row_count += 1
                    
    except Exception as e:
        print(f"Error extracting {xlsx_file}: {e}\\n")

if __name__ == "__main__":
    files = glob.glob("*.xlsx")
    for f in files:
        extract_first_rows(f)
