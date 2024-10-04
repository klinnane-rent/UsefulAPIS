const OPENAI_API_KEY = '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';
const userContent = document.body.innerText;

function chatGptRequest() {
    if (new TextEncoder().encode(userContent).length > 8191) { // Checking byte length approximation for tokens
        // Function to break the string into chunks
        function chunkString(str, size) {
            const numChunks = Math.ceil(str.length / size);
            const chunks = new Array(numChunks);

            for (let i = 0, o = 0; i < numChunks; ++i, o += size) {
                chunks[i] = str.substring(o, o + size);
            }
            return chunks;
        }

        // Divide content into manageable chunks
        const chunks = chunkString(userContent, 8191);

        // Function to request batch embeddings for all chunks
        function chatGptBatchRequest(chunks) {
            return fetch('https://api.openai.com/v1/embeddings', {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${OPENAI_API_KEY}`,
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    model: "text-embedding-3-small",
                    input: chunks,
                    encoding_format: "float",
                })
            })
            .then(response => {
                if (!response.ok) {
                    return response.text().then(text => { throw new Error(text); });
                }
                return response.json();
            })
            .then(data => {
                if (data.data.length > 0) {
                    const numEmbeddings = data.data.length;
                    const embeddingLength = data.data[0].embedding.length;
                    const sumEmbedding = new Array(embeddingLength).fill(0);

                    data.data.forEach(embed => {
                        embed.embedding.forEach((value, index) => {
                            sumEmbedding[index] += value;
                        });
                    });

                    const averageEmbedding = sumEmbedding.map(sum => sum / numEmbeddings);
                    return averageEmbedding.toString();
                } else {
                    throw new Error("No embeddings returned from the API.");
                }
            });
        }

        // Make a single batch request with all chunks and process the average
        return chatGptBatchRequest(chunks);
    } else {
        // Process single embedding request if content is within the token limit
        return fetch('https://api.openai.com/v1/embeddings', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${OPENAI_API_KEY}`,
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                model: "text-embedding-3-small",
                input: userContent,
                encoding_format: "float",
            })
        })
        .then(response => {
            if (!response.ok) {
                 return response.text().then(text => {throw new Error(text)});
            }
            return response.json();
        })
        .then(data => {
            console.log(data.data[0].embedding);
            return data.data[0].embedding.toString();
        });
    }
}

// Execute request and handle results
return chatGptRequest()
    .then(embeddings => seoSpider.data(embeddings))
    .catch(error => seoSpider.error(error));
