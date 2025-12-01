"""
Утилита для конвертации .doc файлов в .docx формат.

Использование:
    python convert_doc_to_docx.py                    # Конвертировать все .doc в input/
    python convert_doc_to_docx.py --delete-original  # Удалить оригиналы после конвертации
    python convert_doc_to_docx.py path/to/file.doc   # Конвертировать один файл
"""

import os
import sys
import argparse
from pathlib import Path


def convert_doc_to_docx(doc_path, docx_path=None, verbose=True):
    """
    Конвертирует .doc в .docx через Microsoft Word.
    
    Args:
        doc_path: путь к .doc файлу
        docx_path: путь для сохранения .docx (если None, заменит расширение)
        verbose: выводить ли сообщения
    
    Returns:
        str: путь к созданному .docx файлу или None при ошибке
    """
    if docx_path is None:
        docx_path = str(Path(doc_path).with_suffix('.docx'))
    
    # Формат 16 = wdFormatXMLDocument (docx)
    wdFormatDocx = 16
    
    try:
        import win32com.client
    except ImportError:
        if verbose:
            print("Ошибка: библиотека pywin32 не установлена")
            print("Установите: pip install pywin32")
        return None
    
    word = None
    try:
        word = win32com.client.Dispatch("Word.Application")
        word.Visible = False
        word.DisplayAlerts = 0
        
        doc = word.Documents.Open(os.path.abspath(doc_path))
        doc.SaveAs(os.path.abspath(docx_path), FileFormat=wdFormatDocx)
        doc.Close()
        
        if verbose:
            print(f"✓ {os.path.basename(doc_path)} → {os.path.basename(docx_path)}")
        return docx_path
        
    except Exception as e:
        if verbose:
            print(f"✗ Ошибка при конвертации {doc_path}: {e}")
        return None
        
    finally:
        if word:
            try:
                word.Quit()
            except:
                pass


def convert_all_doc_in_folder(folder_path, delete_original=False, verbose=True):
    """
    Конвертирует все .doc файлы в папке в .docx (рекурсивно).
    
    Args:
        folder_path: путь к папке
        delete_original: удалить ли оригинальные .doc файлы после конвертации
        verbose: выводить ли сообщения
    
    Returns:
        int: количество сконвертированных файлов
    """
    doc_files = list(Path(folder_path).rglob("*.doc"))
    # Пропускаем временные файлы Word
    doc_files = [f for f in doc_files if not f.name.startswith('~$')]
    
    if verbose:
        print(f"\nНайдено {len(doc_files)} .doc файлов в {folder_path}")
    
    converted = 0
    skipped = 0
    
    for doc_file in doc_files:
        docx_file = doc_file.with_suffix('.docx')
        
        # Пропускаем если .docx уже существует
        if docx_file.exists():
            if verbose:
                print(f"⊘ {doc_file.name} (уже есть .docx)")
            skipped += 1
            continue
        
        result = convert_doc_to_docx(str(doc_file), str(docx_file), verbose=verbose)
        
        if result:
            converted += 1
            if delete_original:
                try:
                    doc_file.unlink()
                    if verbose:
                        print(f"  Удален оригинал: {doc_file.name}")
                except Exception as e:
                    if verbose:
                        print(f"  Не удалось удалить оригинал: {e}")
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Конвертировано: {converted}")
        print(f"Пропущено (уже .docx): {skipped}")
        print(f"Всего: {len(doc_files)}")
        print(f"{'='*60}")
    
    return converted


def main():
    parser = argparse.ArgumentParser(
        description="Конвертация .doc файлов в .docx для ускорения обработки"
    )
    
    parser.add_argument(
        'path',
        nargs='?',
        default='input',
        help='Путь к папке или файлу (по умолчанию: input/)'
    )
    
    parser.add_argument(
        '--delete-original',
        action='store_true',
        help='Удалить оригинальные .doc файлы после конвертации'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Не выводить сообщения'
    )
    
    args = parser.parse_args()
    
    path = Path(args.path)
    
    if not path.exists():
        print(f"Ошибка: путь {path} не существует")
        return 1
    
    if path.is_file():
        if path.suffix.lower() == '.doc':
            result = convert_doc_to_docx(str(path), verbose=not args.quiet)
            return 0 if result else 1
        else:
            print(f"Ошибка: файл {path} не является .doc файлом")
            return 1
    
    elif path.is_dir():
        converted = convert_all_doc_in_folder(
            str(path),
            delete_original=args.delete_original,
            verbose=not args.quiet
        )
        return 0 if converted >= 0 else 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
