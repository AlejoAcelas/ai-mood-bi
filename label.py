from langchain_core.output_parsers import JsonOutputParser
from langchain_openai import OpenAI
from langchain.prompts import PromptTemplate
import ratelimit
from constants import ONE_MINUTE, OPENAI_API_KEY, OPENAI_API_RATE_LIMIT

def initialize_labeling_llm_chain(
    prompt_template_path: str = 'label-prompt.txt',
    open_ai_api_key: str = OPENAI_API_KEY,
    model_name: str = 'gpt-3.5-turbo-0125',
    max_tokens: int = 2048,
    temperature: int = 0,    
):
    model = OpenAI(
        name=model_name,
        api_key=open_ai_api_key,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    with open(prompt_template_path, 'r') as file:
        template = file.read()
    parser = JsonOutputParser()
    
    prompt = PromptTemplate(
        template=template,
        input_variables=['llm_json'],
    )
    chain = prompt | model | parser
    return chain

LLM_CHAIN = initialize_labeling_llm_chain()

@ratelimit.sleep_and_retry
@ratelimit.limits(calls=OPENAI_API_RATE_LIMIT, period=ONE_MINUTE)
async def get_content_labels(llm_json):
    label_json = await LLM_CHAIN.ainvoke({'llm_json': llm_json})
    return label_json