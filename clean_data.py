import pandas as pd
import numpy as np
import os, re

filename = 'private_repo/clean_data/cleaned_new_bags.csv'

if os.path.exists(filename):
    os.remove(filename)

fabrics = 'cotton-blend, cotton, monogram, denim, recycled leather, raffia/calf leather, faux-leather, faux leather, nappa leather, calf leather, canvas/calfskin, raffia/leather, canvas/calf leather, artificial leather, canvas leather, mahogany leather, leather, grain, lambskin, calfskin, canvas jacquard, jacquard, canvas, recycled polyester, satin, suede, silk satin, braided motif, sheepskin, lambskin, twill, raffia, pebble, recycled nylon'.split(', ')
fabrics += ['brass', 'sheep shearling', 'buffalo leather', 'synthetic raffia', 'sheep shearling; calfskin details',
    'cow leather', 'cowhide leather', 'faux leather', 'lambskin', 'bull leather', 'goatskin', 'lamb shearling',
    'calfskin details', 'calf hair', 'calfskin', 'deerskin', 'bovine leather']

english_colors = [
    "Grey", "Black", "Powder", "Light Grey", "Brown", "Light brown", 
    "Dove grey", "White", "Pink", '', "Orange", "Green", "Silver", 
    "Camel", "Light blue", "Lilac", "Blue", "Khaki", "Yellow", "Caramel", 
    "Purple", "Bronze", "Fuchsia", "Red", "Dark blue", "Cream", "Leopard", 
    "Bordeaux", 'Gold', 'Bronze'
]

italian_colors = [
    "Grigio", "Nero", "Cipria", "Grigio Chiaro", "Marrone", "Marrone Chiaro", 
    "Tortora", "Bianco", "Rosa", "Cuoio", "Arancio", "Verde", "Argento", 
    "Cammello", "Azzurro", "Lilla", "Blu", "Kaki", "Giallo", "Caramello", 
    "Viola", "Bronzo", "Fucsia", "Rosso", "Blu scuro", "Panna", "Leopardato", 
    "Bordeaux", 'Oro', 'Bronzo'
]

BAG_MAPPING = {
    'shopping bag': 'Apparel & Accessories > Handbags, Wallets & Cases > Handbags > Shopper Bags',
    'shoulder bag': 'Apparel & Accessories > Handbags, Wallets & Cases > Handbags > Shoulder Bags',
    'bucket bag': 'Apparel & Accessories > Handbags, Wallets & Cases > Handbags > Bucket Bags',
    'handbags': 'Apparel & Accessories > Handbags, Wallets & Cases > Handbags',
    'crossbody bag': 'Apparel & Accessories > Handbags, Wallets & Cases > Handbags > Cross Body Bags',
    'mini bag': 'Apparel & Accessories > Handbags, Wallets & Cases > Handbags',
    'backpack': 'Luggage & Bags > Backpacks',
    'bag': 'Apparel & Accessories > Handbags, Wallets & Cases',
    'belt bag': 'Apparel & Accessories > Handbags, Wallets & Cases',
    'clutch bag': 'Apparel & Accessories > Handbags, Wallets & Cases > Handbags > Clutch Bags'
}

IT_EN_BAG_MAPPING = {
    'borsa a mano': 'Handbag',
    'borsa a spalla': 'Shoulder Bag',
    'borsa shopping': 'Shopping Bag',
    'borsa a tracolla': 'Crossbody Bag',
    'mini borsa': 'Mini Bag',
    'secchiello': 'Bucket Bag',
    'borse clutch': 'Clutch Bag',
    'marsupio': 'Belt Bag',
    'zaino': 'Backpack',
    'borsa': 'bag'   
}


COLOR_CATEGORIES = {
    'Blue': ['Blue', 'Navy', 'Aqua', 'Denim', 'Indigo', 'Periwinkle', 'Cobalt', 'Sapphire', 'Sky'],
    'Black': ['Black', 'Anthracite', 'Ebony'],
    'Green': ['Green', 'Olive', 'Emerald', 'Mint', 'Forest', 'Lichen'],
    'Red': ['Red', 'Bordeaux', 'Wine', 'Burgundy', 'Garnet', 'Salmon', 'Omnibus', 'Siren'],
    'Pink': ['Pink', 'Rose', 'Blush', 'Fuxia', 'Pansy'],
    'Yellow': ['Yellow', 'Mango', 'Pale Yellow', 'Mustard', 'Mimosa'],
    'White': ['White', 'Ivory', 'Cream', 'Eggshell', 'Optic', 'Snow'],
    'Grey': ['Grey', 'Gray', 'Slate', 'Melange', 'Pewter', 'Granite'],
    'Beige': ['Beige', 'Taupe', 'Camel', 'Sand', 'Tapioca', 'Latte'],
    'Brown': ['Brown', 'Cocoa', 'Chocolate', 'Hazel', 'Tobacco', 'Toffee', 'Mahogany'],
    'Purple': ['Purple', 'Violet', 'Aubergine'],
    'Orange': ['Orange', 'Rust', 'Clay', 'Sudan'],
    'Gold': ['Gold', 'Antique Gold'],
    'Bronze': ['Bronze'],
    'Velvet': ['Velvet', 'Fuchsia'],
    'Silver': ['Silver'],
    'Butter': ['Butter']
}

COLOR_MAP = dict(zip(italian_colors, english_colors))

def get_color(x):    
    try:
        x = x.lower().strip()
        
        if '/' in x:
            return x.title()
        
        for key, values in COLOR_CATEGORIES.items():
            for v in values:
                if v.lower() in x:
                    return key
                
        return x.title()
    except:
        return x
    
def round_to_5_or_0(x):
    return np.round(x / 5) * 5


def fix_vendors(x):
    x = x.lower()
    
    if 'moncler basic' in x:
        x = 'moncler'
    
    if 'self portrait' in x:
        x = 'Self-Portrait'
    
    if 'mm6' in x:
        return 'MM6'

    if 't shirt' in x:
        return 'T-Shirt'
    
    if 'comme de garcons' in x:
        return 'Comme Des Garçons'
    
    if 'carhartt wip' in x:
        
        return 'Carhartt WIP'
    
    if x == '"' or x == "''":
        return ''
    
    if 'golden goose' in x:
        return 'Golden Goose'
    
    return x.title()

def get_fabic(row):
    if type(row) == float: return None
    
    for fabric in fabrics:
        if fabric in row.lower():
            if 'faux' in fabric:
                return 'Faux Leather'
            
            return fabric.title().replace('-', ' ')
    
    return None


def extract_measurements(entry):
    # Default values
    width, height, depth, handle, strap, country = None, None, None, None, None, None
    
    if pd.isna(entry):
        return width, height, depth, handle, strap, country
    
    # Searching for country names (as a rough assumption, country names start with capital letters and are alphabetical)
    country_match = re.search(r'\b[A-Z][a-z]+\b', entry)
    if country_match:
        country = country_match.group(0)
    
    # Extracting width, height, depth, handle, and strap measurements
    entry = entry.lower()  # Lowercase everything for easier matching
    
    # Adding colon as an optional separator and making the regex more flexible
    width_match = re.search(r'(width)\s*[:\s]*([\d.,]+)\s*(in|cm|centimetres|centimeters)', entry)
    height_match = re.search(r'(height)\s*[:\s]*([\d.,]+)\s*(in|cm|centimetres|centimeters)', entry)
    depth_match = re.search(r'(depth)\s*[:\s]*([\d.,]+)\s*(in|cm|centimetres|centimeters)', entry)
    handle_match = re.search(r'(handle)\s*[:\s]*([\d.,]+)\s*(in|cm|centimetres|centimeters)', entry)
    strap_match = re.search(r'(strap)\s*[:\s]*([\d.,]+)\s*(in|cm|centimetres|centimeters)', entry)

    # Get the value directly without conversion
    def get_value(value, unit):
        return f"{value} {unit}"

    # Extract values in the available units (cm or in)
    if width_match:
        width = get_value(width_match.group(2), width_match.group(3))
    if height_match:
        height = get_value(height_match.group(2), height_match.group(3))
    if depth_match:
        depth = get_value(depth_match.group(2), depth_match.group(3))
    if handle_match:
        handle = get_value(handle_match.group(2), handle_match.group(3))
    if strap_match:
        strap = get_value(strap_match.group(2), strap_match.group(3))
    
    return width, height, depth, handle, strap, fix_country(country)



def fix_country(x):
    if type(x) == float or x is None: return x
    
    x = x.title()
    
    if x in ['France', 'Francia']:
        return 'France'
    if x in ['Italy', 'Italia']:
        return 'Italy'
    if x in ['China', 'Cina']:
        return 'China'
    if x in ['Germany', 'Germania']:
        return 'Germany'
    if x in ['Denmark', 'Danimarca']:
        return 'Denmark'
    if x in ['United States', 'Stati Uniti d']:
        return 'United States'
    if x in ['United Kingdom', 'Gran Bretagna']:
        return 'United Kingdom'
    if x in ['Portugal', 'Portogallo']:
        return 'Portugal'
    if x in ['Japan', 'Giappone']:
        return 'Japan'
    if x in ['Poland', 'Polonia']:
        return 'Poland'
    if x in ['Slovenia']:
        return 'Slovenia'
    if x in ['Spain', 'Spagna']:
        return 'Spain'
    if x in ['Australia']:
        return 'Australia'
    if x in ['Hungary', 'Ungheria']:
        return 'Hungary'
    if x in ['Thailand', 'Tailandia']:
        return 'Thailand'
    if x in ['Vietnam']:
        return 'Vietnam'
    if x in ['Romania']:
        return 'Romania'
    if x in ['Bosnia and Herzegovina', 'Bosnia ed Erzegovina']:
        return 'Bosnia and Herzegovina'
    if x in ['Bulgaria']:
        return 'Bulgaria'
    if x in ['Turkey', 'Turchia']:
        return 'Turkey'
    if x in ['Moldova Republic of', 'Moldova', 'Moldavia', 'Moldova@ Republic of']:
        return 'Moldova'
    if x in ['Sweden', 'Svezia']:
        return 'Sweden'
    if x in ['Macedonia']:
        return 'Macedonia'
    if x in ['Philippines', 'Filippine']:
        return 'Philippines'
    if x in ['Tunisia']:
        return 'Tunisia'
    if x in ['Korea, Republic of', 'Corea', 'Korea@ Republic of']:
        return 'South Korea'
    if x in ['Cambodia', 'Cambogia']:
        return 'Cambodia'
    if x in ['Pakistan']:
        return 'Pakistan'
    if x in ['Austria']:
        return 'Austria'
    if x in ['India']:
        return 'India'
    if x in ['Brazil', 'Brasile']:
        return 'Brazil'
    if x in ['Morocco', 'Marocco']:
        return 'Morocco'
    if x in ['Serbia']:
        return 'Serbia'
    if x in ['Ireland', 'Irlanda']:
        return 'Ireland'
    if x in ['Lithuania', 'Lituania']:
        return 'Lithuania'
    if x in ['Indonesia']:
        return 'Indonesia'
    if x in ['Madagascar']:
        return 'Madagascar'
    if x in ['Bangladesh']:
        return 'Bangladesh'
    if x in ['El Salvador']:
        return 'El Salvador'
    if x in ['Albania']:
        return 'Albania'
    if x in ['Jordan', 'Giordania']:
        return 'Jordan'
    if x in ['Mauritius']:
        return 'Mauritius'
    if x in ['Slovakia', 'Slovacchia']:
        return 'Slovakia'
    if x in ["Lao People's Democratic Republic", 'Laos']:
        return 'Laos'
    if x in ['Taiwan']:
        return 'Taiwan'
    if x in ['Myanmar']:
        return 'Myanmar'
    if x in ['Armenia']:
        return 'Armenia'
    if x in ['Hong Kong']:
        return 'Hong Kong'
    if x in ['Sri Lanka']:
        return 'Sri Lanka'
    if x in ['Guatemala']:
        return 'Guatemala'
    if x in ['Peru', 'Perù']:
        return 'Peru'
    if x in ['Kenya', 'Kenia']:
        return 'Kenya'
    if x in ['Belgium', 'Belgio']:
        return 'Belgium'
    if x in ['Switzerland', 'Svizzera']:
        return 'Switzerland'
    if x in ['Mongolia']:
        return 'Mongolia'
    if x in [0, '0']:
        return ''
    return ''


def convert_inches_to_cm(x):
    try:
        # Convert string to float and perform the conversion
        value_in_inches = float(x.replace(',', '.').replace('in', ''))  # Ensure it works with commas as decimal points
        return round(value_in_inches * 2.54, 1)  # Convert to cm and round to 2 decimal places
    except ValueError:
        return None, None

def fix_dimensions(x):
    if type(x) == float or x is None: return 0
    cm = 0
    
    if 'in' in x:
        cm = convert_inches_to_cm(x)
        
    if 'cm' in x:
        try:
            cm = float(x.replace(',', '.').replace('cm', ''))
        except TypeError:
            return 0
        except AttributeError:
            return 0
    return cm


def fix_bags(data):
    bags = data[['Width', 'Height']]
    empty_bags = bags[(bags['Width'] == '0') & (bags['Height'] == '0')]
    data.loc[empty_bags.index, ['Width', 'Height']] = ''
    data['Dimensions'] = data.apply(lambda row: fix_bag_dim(row['Width'], row['Height'], row['Depth']), axis=1)
    return data

def fix_bag_dim(length, height, depth=None):
    # Handle cases where length or height are missing or invalid
    if length == 0 or height == 0 or length == '0' or height == '0' or isinstance(length, int):
        return ''
    
    try:
        # Function to clean and convert the dimensions to float
        remove = lambda z: float(str(z).lower().replace('c', '').replace('m', '').replace(',', '.').replace('!', '').strip())
    
        # Convert length and height
        length, height = remove(length), remove(height)
        
        # Only convert depth if it is provided
        if depth is not None and depth != '' and depth != '0':
            depth = remove(depth)
        else:
            depth = None  # Mark depth as missing if it's not provided
    except ValueError:
        return ''
    
    # Convert to cm
    if depth is not None:
        cm = f'H {height} cm x L {length} cm x D {depth} cm'
    else:
        cm = f'H {height} cm x L {length} cm'
    
    # Conversion function from cm to inches
    cm_to_inch = lambda z: round(z / 2.54, 1)

    # Convert to inches
    length, height = cm_to_inch(length), cm_to_inch(height)
    if depth is not None:
        depth = cm_to_inch(depth)
        inches = f'H {height}" x L {length}" x D {depth}"'
    else:
        inches = f'H {height}" x L {length}"'
    
    # Return the final formatted string with cm and inches
    return f'{cm} / {inches}'


def clean_data():
    
    data = pd.read_csv('private_repo/clean_data/new_bags.csv')
    
    for idx, row in data.iterrows():
        collection = row['Collection'].lower().replace('new', '').replace('collection', '').strip()
        coll = '20' + collection[-2:]
        season = ' - '.join([x.title() for x in collection[:-2].strip().split(' ')])
        
        color = COLOR_MAP.get(row['Color'].title(), row['Color'].title())
        
        fabric = get_fabic(row['Description'])
        color_orig = get_color(color)
        
        width, height, depth, handle, strap, country = extract_measurements(row['Size & Fit'])
        
        if width is None or height is None:
            width, height, depth, handle, strap, _ = extract_measurements(row['Description'])
        
        width = '' if width in [0.0, '0.0', 0, '0'] else width
        height = '' if height in [0.0, '0.0', 0, '0'] else height
        depth = '' if depth in [0.0, '0.0', 0, '0'] else depth
        
        made_in = fix_country(row['Made in'])
        
        product_type = IT_EN_BAG_MAPPING.get(row['Product Type'].lower(), None)
        
        category = BAG_MAPPING.get(IT_EN_BAG_MAPPING.get(row['Product Type'].lower(), None).lower(), row['Product Type'].title())
        
        try:
            retail_price = float(row['Discounted Price'])
        except:
            retail_price = float(str(row['Discounted Price']).replace('"', '').replace('.', '').replace(',', '.'))
        
        try:
            compare_price = float(row['Price'])
        except:
            compare_price = float(str(row['Price']).replace('"', '').replace('.', '').replace(',', '.'))
        
        if retail_price == 0 or retail_price is None or retail_price == '':
            continue
        
        retail_price *= 1.08
        retail_price = round(retail_price, 2)
        unit_cost = retail_price
        retail_price *= 1.25
        compare_price *= 1.45
        
        retail_price = round_to_5_or_0(retail_price)
        compare_price = round_to_5_or_0(compare_price)
        
        images = []
        
        if pd.isna(row['Images']): 
            print(f'Product does not have images, skipping...')
            continue
        
        try:
            for img in row['Images'].split(','):
                images.append(img)
        except AttributeError:
            continue
        
        if color:
            tags = f'{color},{product_type},{season},{coll},Final Sale,Beatsrl'
        else:
            tags = f'{product_type},{season},{coll},Final Sale,Beatsrl'
        
        title = row['Product Title']
        
        
        if pd.isna(title):
            if fabric:
                title = f'{color} {fabric} {product_type}'.replace('Clutche', 'Clutch').strip().title()
            else:
                title = f'{color} {product_type}'.replace('Clutche', 'Clutch').strip().title()
        
        pd.DataFrame({
            'Product Title': [title.title()],
            'Vendor': [fix_vendors(row['Vendor']).upper()],
            'SKU': [row['SKU']],
            'Supplier SKU': [row['SKU']],
            'Unit Cost': [unit_cost],
            'Retail Price': [retail_price],
            'Compare At Price': [compare_price],
            'Material': [fabric],
            'Color detail': [color_orig],
            'Color Supplier': [color.title()],
            'Country': [made_in if type(made_in) != float else country],
            'Tags': [tags],
            'Product Category': [category],
            'Year': [coll],
            'Season': [season],
            'Size': ['OS'],
            'Qty': [2],
            'Width': [fix_dimensions(width)],
            'Height': [fix_dimensions(height)],
            'Depth': [fix_dimensions(depth)],
            'Handle': [fix_dimensions(handle)],
            'Strap': [fix_dimensions(strap)],
            'Description': [row['Description']],
            'Clean Images': [','.join(images)]
            
            # 'Product Category': 
        }).to_csv(filename, index=False, mode='a', header=not os.path.exists(filename))
        

def final_prep():
    clean_data()
    
    skus_path = 'private_repo/clean_data/all_skus.csv'
    
    data = pd.read_csv(filename)
    data = fix_bags(data)
    
    data.dropna(subset=['Retail Price', 'Unit Cost', 'Compare At Price'], inplace=True)
    
    if not os.path.exists(skus_path):
        all_skus = data[['SKU']]
    else:
        all_skus = pd.read_csv(skus_path)
        
        # get products to create
        to_create = data[~data['SKU'].isin(all_skus['SKU'])]
        to_create.to_csv('private_repo/clean_data/to_create.csv', index=False)
        
        all_skus = pd.concat([all_skus['SKU'], data['SKU']], ignore_index=True).drop_duplicates()
        all_skus = pd.DataFrame({'SKU': all_skus.values.tolist()})
        
        zero_inventory = all_skus[~all_skus['SKU'].isin(data['SKU'])]
        zero_inventory.to_csv('private_repo/clean_data/zero_inventory.csv', index=False)
    
    if all_skus.shape[1] > 1:
        all_skus = pd.DataFrame(all_skus.stack().reset_index(drop=True), columns=['SKU'])
    
    # save all skus
    all_skus.to_csv(skus_path, index=False)
    
    data.to_csv(filename, index=False)