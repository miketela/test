#!/usr/bin/env python3
"""
PDF Report Generation for SBP Atoms Pipeline.
Generates comprehensive exploration and analysis reports.
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, Image, KeepTogether
)
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.piecharts import Pie
from reportlab.lib.colors import HexColor

import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import base64


@dataclass
class ReportConfig:
    """Configuration for PDF report generation."""
    title: str
    subtitle: str = ""
    author: str = "SBP Atoms Pipeline"
    subject: str = "Data Exploration Report"
    page_size: tuple = A4
    margins: Dict[str, float] = None
    
    def __post_init__(self):
        if self.margins is None:
            self.margins = {
                'top': 1 * inch,
                'bottom': 1 * inch,
                'left': 0.75 * inch,
                'right': 0.75 * inch
            }


class PDFReportGenerator:
    """Generates PDF reports from exploration metrics and data analysis."""
    
    def __init__(self, config: ReportConfig):
        """Initialize PDF report generator.
        
        Args:
            config: Report configuration
        """
        self.config = config
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
        
    def _setup_custom_styles(self):
        """Setup custom paragraph styles."""
        # Title style
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Title'],
            fontSize=24,
            spaceAfter=30,
            textColor=colors.darkblue,
            alignment=1  # Center
        ))
        
        # Subtitle style
        self.styles.add(ParagraphStyle(
            name='CustomSubtitle',
            parent=self.styles['Normal'],
            fontSize=14,
            spaceAfter=20,
            textColor=colors.grey,
            alignment=1  # Center
        ))
        
        # Section header style
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading1'],
            fontSize=16,
            spaceAfter=12,
            spaceBefore=20,
            textColor=colors.darkblue,
            borderWidth=1,
            borderColor=colors.darkblue,
            borderPadding=5
        ))
        
        # Subsection header style
        self.styles.add(ParagraphStyle(
            name='SubsectionHeader',
            parent=self.styles['Heading2'],
            fontSize=14,
            spaceAfter=8,
            spaceBefore=12,
            textColor=colors.darkgreen
        ))
    
    def generate_exploration_report(self, 
                                  metrics_file: Path, 
                                  output_file: Path,
                                  raw_data_dir: Optional[Path] = None) -> bool:
        """Generate exploration report from metrics file.
        
        Args:
            metrics_file: Path to exploration metrics JSON file
            output_file: Path for output PDF file
            raw_data_dir: Optional path to raw data directory for additional analysis
            
        Returns:
            True if report generated successfully
        """
        try:
            # Load metrics data
            with open(metrics_file, 'r', encoding='utf-8') as f:
                metrics = json.load(f)
            
            # Create PDF document
            doc = SimpleDocTemplate(
                str(output_file),
                pagesize=self.config.page_size,
                topMargin=self.config.margins['top'],
                bottomMargin=self.config.margins['bottom'],
                leftMargin=self.config.margins['left'],
                rightMargin=self.config.margins['right'],
                title=self.config.title,
                author=self.config.author,
                subject=self.config.subject
            )
            
            # Build report content
            story = []
            
            # Title page
            self._add_title_page(story, metrics)
            
            # Executive summary
            self._add_executive_summary(story, metrics)
            
            # File analysis section
            self._add_file_analysis(story, metrics, raw_data_dir)
            
            # Data quality section
            self._add_data_quality_section(story, metrics)
            
            # Column analysis section
            self._add_column_analysis(story, metrics)
            
            # Appendix
            self._add_appendix(story, metrics)
            
            # Build PDF
            doc.build(story)
            return True
            
        except Exception as e:
            print(f"Error generating PDF report: {e}")
            return False
    
    def _add_title_page(self, story: List, metrics: Dict[str, Any]):
        """Add title page to report."""
        # Main title
        title = f"Exploración de Datos - {metrics['atom']}"
        story.append(Paragraph(title, self.styles['CustomTitle']))
        
        # Subtitle with period
        subtitle = f"Período: {metrics['period']} | Run ID: {metrics['run_id']}"
        story.append(Paragraph(subtitle, self.styles['CustomSubtitle']))
        
        story.append(Spacer(1, 0.5 * inch))
        
        # Summary table
        summary_data = [
            ['Métrica', 'Valor'],
            ['Archivos Analizados', str(metrics['files_analyzed'])],
            ['Total de Registros', str(metrics.get('total_records', 0))],
            ['Fecha de Generación', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            ['Timestamp de Ejecución', metrics['timestamp']]
        ]
        
        summary_table = Table(summary_data, colWidths=[2.5*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(summary_table)
        story.append(PageBreak())
    
    def _add_executive_summary(self, story: List, metrics: Dict[str, Any]):
        """Add executive summary section."""
        story.append(Paragraph("Resumen Ejecutivo", self.styles['SectionHeader']))
        
        # Generate summary text based on metrics
        files_count = metrics['files_analyzed']
        total_records = metrics.get('total_records', 0)
        atom_type = metrics['atom']
        period = metrics['period']
        
        summary_text = f"""
        Este reporte presenta los resultados de la exploración de datos para el átomo {atom_type} 
        correspondiente al período {period}. Se analizaron {files_count} archivo(s) con un total 
        de {total_records} registros.
        
        La exploración incluye análisis de calidad de datos, estructura de archivos, 
        distribución de valores y validaciones de esquema.
        """
        
        story.append(Paragraph(summary_text, self.styles['Normal']))
        story.append(Spacer(1, 0.3 * inch))
    
    def _add_file_analysis(self, story: List, metrics: Dict[str, Any], raw_data_dir: Optional[Path]):
        """Add file analysis section organized by file type."""
        story.append(Paragraph("Análisis de Archivos", self.styles['SectionHeader']))
        
        file_metrics = metrics.get('file_metrics', {})
        
        # Group files by type based on expected_files.json subtypes
        file_groups = {
            'BASE_AT12': {},
            'TDC_AT12': {},
            'VALORES_AT12': {},
            'SOBREGIRO_AT12': {},
            'GARANTIA_AUTOS_AT12': {},
            'POLIZA_HIPOTECAS_AT12': {},
            'AFECTACIONES_AT12': {},
            'VALOR_MINIMO_AVALUO_AT12': {},
            'OTROS': {}
        }
        
        for filename, file_data in file_metrics.items():
            filename_upper = filename.upper()
            if 'BASE_AT12' in filename_upper:
                file_groups['BASE_AT12'][filename] = file_data
            elif 'TDC_AT12' in filename_upper:
                file_groups['TDC_AT12'][filename] = file_data
            elif 'VALORES_AT12' in filename_upper:
                file_groups['VALORES_AT12'][filename] = file_data
            elif 'SOBREGIRO_AT12' in filename_upper:
                file_groups['SOBREGIRO_AT12'][filename] = file_data
            elif 'GARANTIA_AUTOS_AT12' in filename_upper:
                file_groups['GARANTIA_AUTOS_AT12'][filename] = file_data
            elif 'POLIZA_HIPOTECAS_AT12' in filename_upper:
                file_groups['POLIZA_HIPOTECAS_AT12'][filename] = file_data
            elif 'AFECTACIONES_AT12' in filename_upper:
                file_groups['AFECTACIONES_AT12'][filename] = file_data
            elif 'VALOR_MINIMO_AVALUO_AT12' in filename_upper:
                file_groups['VALOR_MINIMO_AVALUO_AT12'][filename] = file_data
            else:
                file_groups['OTROS'][filename] = file_data
        
        # Process each group
        for group_name, group_files in file_groups.items():
            if not group_files:  # Skip empty groups
                continue
                
            # Add group header
            group_title = f"Archivos {group_name}"
            story.append(Paragraph(group_title, self.styles['SubsectionHeader']))
            
            for filename, file_data in group_files.items():
                story.append(Paragraph(f"• {filename}", self.styles['Normal']))
                
                # File details table
                file_details = [
                    ['Propiedad', 'Valor'],
                    ['Ruta', file_data.get('file_path', 'N/A')],
                    ['Tamaño', f"{file_data.get('file_size', 0):,} bytes"],
                    ['Última Modificación', file_data.get('file_mtime', 'N/A')],
                    ['SHA256', file_data.get('file_sha256', 'N/A')[:16] + '...'],
                    ['Filas', str(file_data.get('row_count', 0))],
                    ['Columnas', str(file_data.get('column_count', 0))]
                ]
                
                file_table = Table(file_details, colWidths=[2*inch, 3.5*inch])
                file_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 10),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP')
                ]))
                
                story.append(file_table)
                story.append(Spacer(1, 0.15 * inch))
            
            story.append(Spacer(1, 0.2 * inch))  # Extra space between groups
    
    def _add_data_quality_section(self, story: List, metrics: Dict[str, Any]):
        """Add data quality analysis section organized by file type."""
        story.append(Paragraph("Análisis de Calidad de Datos", self.styles['SectionHeader']))
        
        file_metrics = metrics.get('file_metrics', {})
        
        # Group files by type based on expected_files.json subtypes
        file_groups = {
            'BASE_AT12': {},
            'TDC_AT12': {},
            'VALORES_AT12': {},
            'SOBREGIRO_AT12': {},
            'GARANTIA_AUTOS_AT12': {},
            'POLIZA_HIPOTECAS_AT12': {},
            'AFECTACIONES_AT12': {},
            'VALOR_MINIMO_AVALUO_AT12': {},
            'OTROS': {}
        }
        
        for filename, file_data in file_metrics.items():
            filename_upper = filename.upper()
            if 'BASE_AT12' in filename_upper:
                file_groups['BASE_AT12'][filename] = file_data
            elif 'TDC_AT12' in filename_upper:
                file_groups['TDC_AT12'][filename] = file_data
            elif 'VALORES_AT12' in filename_upper:
                file_groups['VALORES_AT12'][filename] = file_data
            elif 'SOBREGIRO_AT12' in filename_upper:
                file_groups['SOBREGIRO_AT12'][filename] = file_data
            elif 'GARANTIA_AUTOS_AT12' in filename_upper:
                file_groups['GARANTIA_AUTOS_AT12'][filename] = file_data
            elif 'POLIZA_HIPOTECAS_AT12' in filename_upper:
                file_groups['POLIZA_HIPOTECAS_AT12'][filename] = file_data
            elif 'AFECTACIONES_AT12' in filename_upper:
                file_groups['AFECTACIONES_AT12'][filename] = file_data
            elif 'VALOR_MINIMO_AVALUO_AT12' in filename_upper:
                file_groups['VALOR_MINIMO_AVALUO_AT12'][filename] = file_data
            else:
                file_groups['OTROS'][filename] = file_data
        
        # Process each group
        for group_name, group_files in file_groups.items():
            if not group_files:  # Skip empty groups
                continue
                
            # Add group header
            group_title = f"Calidad de Datos - {group_name}"
            story.append(Paragraph(group_title, self.styles['SubsectionHeader']))
            
            for filename, file_data in group_files.items():
                quality_metrics = file_data.get('quality_metrics', {})
                
                if quality_metrics:
                    story.append(Paragraph(f"• {filename}", self.styles['Normal']))
                    
                    # Quality metrics table
                    quality_data = [['Métrica', 'Valor', 'Porcentaje']]
                    
                    total_cells = file_data.get('row_count', 0) * file_data.get('column_count', 0)
                    
                    for metric, value in quality_metrics.items():
                        if isinstance(value, (int, float)):
                            percentage = f"{(value/total_cells)*100:.2f}%" if total_cells > 0 else "0%"
                            quality_data.append([metric.replace('_', ' ').title(), str(value), percentage])
                    
                    if len(quality_data) > 1:
                        quality_table = Table(quality_data, colWidths=[2*inch, 1.5*inch, 1.5*inch])
                        quality_table.setStyle(TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgreen),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('FONTSIZE', (0, 0), (-1, -1), 10),
                            ('GRID', (0, 0), (-1, -1), 1, colors.black)
                        ]))
                        
                        story.append(quality_table)
                        story.append(Spacer(1, 0.15 * inch))
            
            story.append(Spacer(1, 0.2 * inch))  # Extra space between groups
    
    def _add_column_analysis(self, story: List, metrics: Dict[str, Any]):
        """Add column analysis section organized by file type."""
        story.append(Paragraph("Análisis de Columnas", self.styles['SectionHeader']))
        
        file_metrics = metrics.get('file_metrics', {})
        
        # Group files by type based on expected_files.json subtypes
        file_groups = {
            'BASE_AT12': {},
            'TDC_AT12': {},
            'VALORES_AT12': {},
            'SOBREGIRO_AT12': {},
            'GARANTIA_AUTOS_AT12': {},
            'POLIZA_HIPOTECAS_AT12': {},
            'AFECTACIONES_AT12': {},
            'VALOR_MINIMO_AVALUO_AT12': {},
            'OTROS': {}
        }
        
        for filename, file_data in file_metrics.items():
            filename_upper = filename.upper()
            if 'BASE_AT12' in filename_upper:
                file_groups['BASE_AT12'][filename] = file_data
            elif 'TDC_AT12' in filename_upper:
                file_groups['TDC_AT12'][filename] = file_data
            elif 'VALORES_AT12' in filename_upper:
                file_groups['VALORES_AT12'][filename] = file_data
            elif 'SOBREGIRO_AT12' in filename_upper:
                file_groups['SOBREGIRO_AT12'][filename] = file_data
            elif 'GARANTIA_AUTOS_AT12' in filename_upper:
                file_groups['GARANTIA_AUTOS_AT12'][filename] = file_data
            elif 'POLIZA_HIPOTECAS_AT12' in filename_upper:
                file_groups['POLIZA_HIPOTECAS_AT12'][filename] = file_data
            elif 'AFECTACIONES_AT12' in filename_upper:
                file_groups['AFECTACIONES_AT12'][filename] = file_data
            elif 'VALOR_MINIMO_AVALUO_AT12' in filename_upper:
                file_groups['VALOR_MINIMO_AVALUO_AT12'][filename] = file_data
            else:
                file_groups['OTROS'][filename] = file_data
        
        # Process each group
        for group_name, group_files in file_groups.items():
            if not group_files:  # Skip empty groups
                continue
                
            # Add group header
            group_title = f"Análisis de Columnas - {group_name}"
            story.append(Paragraph(group_title, self.styles['SubsectionHeader']))
            
            for filename, file_data in group_files.items():
                headers = file_data.get('headers', [])
                column_metrics = file_data.get('column_metrics', [])
                total_rows = file_data.get('row_count', 0)
                
                if headers:
                    story.append(Paragraph(f"• {filename}", self.styles['Normal']))
                    
                    # Create a mapping from column metrics list
                    col_metrics_dict = {}
                    if isinstance(column_metrics, list):
                        for col_metric in column_metrics:
                            if isinstance(col_metric, dict) and 'name' in col_metric:
                                col_metrics_dict[col_metric['name']] = col_metric
                    
                    # Estadísticas detalladas estilo pandas describe
                    story.append(Paragraph("Estadísticas Detalladas por Columna", self.styles['Normal']))
                    
                    stats_data = [['Columna', 'Tipo', 'Count', 'Distinct', 'Min', 'Max', 'Avg', 'Sum', 'Nulls']]
                    
                    for header in headers:
                        col_info = col_metrics_dict.get(header, {})
                        data_type = str(col_info.get('data_type', 'object'))
                        null_count = int(col_info.get('null_count', 0))
                        unique_count = int(col_info.get('unique_count', 0))
                        min_value = col_info.get('min_value')
                        max_value = col_info.get('max_value')
                        mean_value = col_info.get('mean_value')
                        
                        # Compute sum for numeric columns
                        sum_value = None
                        non_null = max(total_rows - null_count, 0)
                        if isinstance(mean_value, (int, float)) and non_null > 0:
                            try:
                                sum_value = mean_value * non_null
                            except Exception:
                                sum_value = None
                        
                        # Format values for display
                        min_str = str(min_value) if min_value is not None else 'N/A'
                        max_str = str(max_value) if max_value is not None else 'N/A'
                        avg_str = f"{mean_value:.2f}" if isinstance(mean_value, (int, float)) else 'N/A'
                        sum_str = f"{sum_value:.2f}" if isinstance(sum_value, (int, float)) else 'N/A'
                        
                        # Truncate long strings for better display
                        if len(min_str) > 15:
                            min_str = min_str[:12] + '...'
                        if len(max_str) > 15:
                            max_str = max_str[:12] + '...'
                        
                        stats_data.append([
                            header[:20] + '...' if len(header) > 20 else header,
                            data_type[:8],
                            str(total_rows),
                            str(unique_count),
                            min_str,
                            max_str,
                            avg_str,
                            sum_str,
                            str(null_count)
                        ])
                    
                    stats_table = Table(stats_data, colWidths=[
                        1.5*inch, 0.6*inch, 0.5*inch, 0.5*inch, 
                        0.8*inch, 0.8*inch, 0.6*inch, 0.8*inch, 0.5*inch
                    ])
                    stats_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 8),
                        ('FONTSIZE', (0, 1), (-1, -1), 7),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
                    ]))
                    
                    story.append(stats_table)
                    story.append(Spacer(1, 0.2 * inch))
            
            story.append(Spacer(1, 0.3 * inch))  # Extra space between groups
    
    def _add_appendix(self, story: List, metrics: Dict[str, Any]):
        """Add appendix with technical details."""
        story.append(PageBreak())
        story.append(Paragraph("Apéndice Técnico", self.styles['SectionHeader']))
        
        # Raw metrics as formatted JSON
        story.append(Paragraph("Métricas Completas (JSON)", self.styles['SubsectionHeader']))
        
        # Format JSON for display
        json_text = json.dumps(metrics, indent=2, ensure_ascii=False)
        
        # Split into chunks to avoid overly long paragraphs
        lines = json_text.split('\n')
        chunk_size = 50
        
        for i in range(0, len(lines), chunk_size):
            chunk = '\n'.join(lines[i:i+chunk_size])
            story.append(Paragraph(f"<pre>{chunk}</pre>", self.styles['Code']))
            if i + chunk_size < len(lines):
                story.append(Spacer(1, 0.1 * inch))


def create_exploration_report(metrics_file: Path, 
                            output_file: Path,
                            title: str = None,
                            raw_data_dir: Path = None) -> bool:
    """Convenience function to create exploration report.
    
    Args:
        metrics_file: Path to exploration metrics JSON file
        output_file: Path for output PDF file
        title: Optional custom title
        raw_data_dir: Optional path to raw data directory
        
    Returns:
        True if report generated successfully
    """
    # Load metrics to get atom info for title
    try:
        with open(metrics_file, 'r', encoding='utf-8') as f:
            metrics = json.load(f)
        
        atom = metrics.get('atom', 'Unknown')
        period = metrics.get('period', 'Unknown')
        
        if title is None:
            title = f"Reporte de Exploración - {atom} ({period})"
        
        config = ReportConfig(
            title=title,
            subtitle=f"Análisis de datos para {atom} - {period}"
        )
        
        generator = PDFReportGenerator(config)
        return generator.generate_exploration_report(metrics_file, output_file, raw_data_dir)
        
    except Exception as e:
        print(f"Error creating exploration report: {e}")
        return False


def create_exploration_excel_summary(metrics_file: Path, output_file: Path) -> bool:
    """Generate an Excel summary report (per-column table) from metrics JSON.

    The output contains one row per column with the following fields:
    - file_name
    - column
    - count (total rows in file)
    - distinct_count
    - min
    - max
    - avg (mean)
    - sum (computed as mean * non_null_count for numeric columns when available)
    - null_count

    Args:
        metrics_file: Path to exploration metrics JSON file (produced by the explore flow)
        output_file: Path to the resulting Excel file (.xlsx)

    Returns:
        True if the Excel report was generated successfully, False otherwise.
    """
    try:
        # Load metrics
        with open(metrics_file, 'r', encoding='utf-8') as f:
            metrics = json.load(f)

        rows: List[Dict[str, Any]] = []
        file_metrics = metrics.get('file_metrics', {})

        for file_name, fm in file_metrics.items():
            total_rows = int(fm.get('row_count', 0))
            cols = fm.get('column_metrics', []) or []
            for col in cols:
                null_count = int(col.get('null_count', 0))
                unique_count = int(col.get('unique_count', 0))
                min_value = col.get('min_value')
                max_value = col.get('max_value')
                mean_value = col.get('mean_value')
                data_type = (col.get('data_type') or '').lower()

                # Compute sum only when we have a numeric mean and non-null count
                sum_value: Optional[float] = None
                non_null = max(total_rows - null_count, 0)
                if isinstance(mean_value, (int, float)):
                    try:
                        sum_value = mean_value * non_null
                    except Exception:
                        sum_value = None

                rows.append({
                    'file_name': file_name,
                    'column': col.get('name'),
                    'data_type': data_type,
                    'count': total_rows,
                    'distinct_count': unique_count,
                    'min': min_value,
                    'max': max_value,
                    'avg': mean_value,
                    'sum': sum_value,
                    'null_count': null_count,
                })

        # Build DataFrame
        df = pd.DataFrame(rows, columns=[
            'file_name', 'column', 'data_type', 'count', 'distinct_count', 'min', 'max', 'avg', 'sum', 'null_count'
        ])

        # Ensure output directory exists
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to Excel
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='summary', index=False)

        return True
    except Exception as e:
        print(f"Error generating Excel summary: {e}")
        return False