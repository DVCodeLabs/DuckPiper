import { AIProvider } from './provider';

export class OllamaProvider implements AIProvider {
    async complete(_prompt: string): Promise<string> { return ''; }
}
