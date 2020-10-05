#https://aws.amazon.com/blogs/big-data/introduction-to-python-udfs-in-amazon-redshift/
import hashlib
import re

class DataMasker:

    def __init__(self):
        pass


    def get_masked_value(self, columnValue, column_length,data_type):
        if not columnValue:
            return columnValue
        column_val = str(columnValue)
        strip_unicode = re.compile("([^-_a-zA-Z0-9!@#%&=,/'\";:~`\$\^\*\(\)\+\[\]\.\{\}\|\?\<\>\\]+|[^\s]+)")
        column_value = strip_unicode.sub('',  column_val)
        domainIndex = column_value.find("@")
        if (domainIndex != -1):
            email_id = column_value[:domainIndex]
            domain = column_value[domainIndex:]
            hashed_email_id = hashlib.sha256(email_id.encode()).hexdigest()
            maskedEmailValue = hashed_email_id + domain
            if len(maskedEmailValue) > column_length:
                domainLen = len(domain)
                requiredHashLen = column_length - domainLen
                if requiredHashLen>=0:
                    requiredHashedEmail = hashed_email_id[:requiredHashLen]
                    return requiredHashedEmail + domain
                else:
                    return ""
            else:
                return maskedEmailValue

        else:
            if (data_type == "varchar"):
                result = hashlib.sha256(column_value.encode())
                maskedStringValue = result.hexdigest()
                if len(maskedStringValue) > column_length:
                    return maskedStringValue[:column_length]
                else:
                    return maskedStringValue
            if (data_type == "numeric"):
                strHashValue = str(int(hashlib.sha256(column_value.encode()).hexdigest(), 16))
                if column_length<len(strHashValue):
                    return strHashValue[:column_length]
                else:
                    return strHashValue
            if (data_type == "integer"):
                strHashValue = str(int(hashlib.sha256(column_value.encode()).hexdigest(), 16))
                return strHashValue[:9]
            if(data_type == "smallint"):
                strHashValue = str(int(hashlib.sha256(column_value.encode()).hexdigest(), 16))
                return strHashValue[:4]
            else:
                return column_val



