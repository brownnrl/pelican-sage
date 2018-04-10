
from collections import namedtuple

from .managefiles import ResultTypes

NT = namedtuple

CellResult = NT('CellResult', 'result_type order data mimetype')
SageError = NT('SageError', 'ename evalue traceback')

CR = CellResult

def combine_results(results):
        # We want to combine all of the text/plains together.
        combined_result = []
        
        if len(results) == 0:
            return []
        
        indx = 0
        accum = ''
        accum_mimetype = ''
        accumulated_mimetypes = ('text/plain', 'text/html')

        if results[0].mimetype in accumulated_mimetypes:
            accum_mimetype = results[0].mimetype

        while indx != len(results):
            current_result = results[indx]
            cr = current_result

            if cr.mimetype == accum_mimetype:
                accum += cr.data
                if accum_mimetype == 'text/html':
                    accum += '<br/>'

                indx += 1
                continue
            elif accum != '':
                combined_result.append(CR(ResultTypes.Stream, len(combined_result), accum, accum_mimetype))
                accum = ''

            if cr.mimetype != accum_mimetype and cr.mimetype in accumulated_mimetypes:
                accum_mimetype = cr.mimetype
                accum += cr.data
                if accum_mimetype == 'text/html':
                    accum += '<br/>'
                indx += 1
                continue


            combined_result.append(CR(cr.result_type, len(combined_result), cr.data, cr.mimetype))
            indx += 1

        if accum != '':
            combined_result.append(CR(ResultTypes.Stream, len(combined_result), accum, accum_mimetype))

        return combined_result
