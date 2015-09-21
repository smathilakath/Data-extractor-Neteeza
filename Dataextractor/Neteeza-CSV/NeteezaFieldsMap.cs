using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Neteeza_CSV
{
    class NeteezaFieldsMap : CsvClassMap<NeteezaFields>
    {
        public NeteezaFieldsMap()
        {
            Map(m => m.AppName).Name("Application Name").Index(0);
            Map(m => m.MasterAppId).Name("Appid").Index(1);
            Map(m => m.AppToken).Name("Application Token").Index(2);
            Map(m => m.Total).Name("Total").Index(3);
            Map(m => m.QboSub).Name("Qbo Sub").Index(4);
            Map(m => m.QboFree).Name("Qbo Free").Index(5);
            Map(m => m.QboTrial).Name("Qbo Trial").Index(6);
        }
    }
}
