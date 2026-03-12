from sentence_transformers import SentenceTransformer

# Initialize the model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Function to print embedding of a given text
def print_embedding(text):
    # Generate the embedding
    embedding = model.encode(text)
    
    # Convert embedding to a comma-separated string
    embedding_str = ', '.join(map(str, embedding))
    
    # Print the embedding
    print(f"Embedding of the given text '{text}':")
    print(embedding_str)
    print(f"Embedding dimensions: {len(embedding)}")

# Example usage
if __name__ == "__main__":
    text = "Kolkata"
    print_embedding(text)
