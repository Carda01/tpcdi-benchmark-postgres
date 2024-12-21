import numpy as np
import pandas as pd
import xmltodict

SF=INPUT_SF

def customermgmt_convert():
    with open('dags/data/sf_'+str(SF)+'/Batch1/CustomerMgmt.xml') as fd:
        cust = xmltodict.parse(fd.read())
        
    actions = cust['TPCDI:Actions']
    action = actions['TPCDI:Action']
    cust_df = pd.DataFrame(columns = np.arange(0, 36))

    rows = []
    for a in action:
        
        cust_row = []
        
        # action element
        cust_row.append(a.get('@ActionType'))
        cust_row.append(a.get('@ActionTS'))
        
        # action.customer element
        cust_row.append(a.get('Customer').get('@C_ID'))
        cust_row.append(a.get('Customer').get('@C_TAX_ID'))
        cust_row.append(a.get('Customer').get('@C_GNDR'))
        cust_row.append(a.get('Customer').get('@C_TIER'))
        cust_row.append(a.get('Customer').get('@C_DOB'))
        
        # action.customer.name element
        if a.get('Customer').get('Name') != None:
            cust_row.append(a.get('Customer').get('Name').get('C_L_NAME'))
            cust_row.append(a.get('Customer').get('Name').get('C_F_NAME'))
            cust_row.append(a.get('Customer').get('Name').get('C_M_NAME'))
        else:
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
        
        # action.customer.address element
        if a.get('Customer').get('Address') != None:
            cust_row.append(a.get('Customer').get('Address').get('C_ADLINE1'))
            cust_row.append(a.get('Customer').get('Address').get('C_ADLINE2'))
            cust_row.append(a.get('Customer').get('Address').get('C_ZIPCODE'))
            cust_row.append(a.get('Customer').get('Address').get('C_CITY'))
            cust_row.append(a.get('Customer').get('Address').get('C_STATE_PROV'))
            cust_row.append(a.get('Customer').get('Address').get('C_CTRY'))
        else:
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            
        # action.customer.contactinfo element
        if a.get('Customer').get('ContactInfo') != None:     
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PRIM_EMAIL'))
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_ALT_EMAIL'))
            
            # action.customer.contactinfo.phone element
            
            # phone_1
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PHONE_1').get('C_CTRY_CODE'))
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PHONE_1').get('C_AREA_CODE'))
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PHONE_1').get('C_LOCAL'))
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PHONE_1').get('C_EXT'))

            # phone_2
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PHONE_2').get('C_CTRY_CODE'))
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PHONE_2').get('C_AREA_CODE'))
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PHONE_2').get('C_LOCAL'))
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PHONE_2').get('C_EXT'))
        
            # phone_3
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PHONE_3').get('C_CTRY_CODE'))
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PHONE_3').get('C_AREA_CODE'))
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PHONE_3').get('C_LOCAL'))
            cust_row.append(a.get('Customer').get('ContactInfo').get('C_PHONE_3').get('C_EXT'))
        else:
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
        
        # action.customer.taxinfo element
        if a.get('Customer').get('TaxInfo') != None:
            cust_row.append(a.get('Customer').get('TaxInfo').get('C_LCL_TX_ID'))
            cust_row.append(a.get('Customer').get('TaxInfo').get('C_NAT_TX_ID'))
        else:
            cust_row.append(None)
            cust_row.append(None)
        
        # action.customer.account attribute
        if a.get('Customer').get('Account') != None:
            cust_row.append(a.get('Customer').get('Account').get('@CA_ID'))
            cust_row.append(a.get('Customer').get('Account').get('@CA_TAX_ST'))
            
            # action.customer.account element
            cust_row.append(a.get('Customer').get('Account').get('CA_B_ID'))
            cust_row.append(a.get('Customer').get('Account').get('CA_NAME'))
        else:
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
            cust_row.append(None)
        rows.append(cust_row)
        

    # Create DataFrame at once and handle missing values
    columns = np.arange(36)
    cust_df = pd.DataFrame(rows, columns=columns).replace([None, "None"], "")
    cust_df.to_csv('dags/data/sf_'+str(SF)+'/Batch1/CustomerMgmt.csv', index = False)
    print('Customer Management data converted from XML to CSV')